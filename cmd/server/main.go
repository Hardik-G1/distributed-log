package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/api"
	"github.com/Hardik-G1/distributed-log/internal/config"
	"github.com/Hardik-G1/distributed-log/internal/raft"
	"github.com/Hardik-G1/distributed-log/internal/state"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg, err := config.ServerConfigLoad(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s, and address %s", cfg.NodeID, cfg.ListenAddr)
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()
	raftStore, err := storage.OpenWALRaftStore(cfg.RaftDataDirectory)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := raftStore.Close(); err != nil {
			log.Printf("close raft store %v", err)
		}
	}()
	applicationStore, err := storage.OpenMemoryApplicationStore(cfg.AppDataDirectory)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := applicationStore.Close(); err != nil {
			log.Printf("close application store %v", err)
		}
	}()
	transport, err := raft.NewGRPCTransport(cfg.PeerAddrs)
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()

	lockManager, err := state.NewLockManager(applicationStore)
	if err != nil {
		log.Fatal(err)
	}
	node, err := raft.NewNode(
		ctx,
		cfg, raftStore,
		lockManager,
		transport,
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := node.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer node.Stop()
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	raftServer, err := api.NewRaftServer(node)
	if err != nil {
		log.Fatal(err)
	}
	pb.RegisterRaftServiceServer(grpcServer, raftServer)
	lockServer, err := api.NewLockServer(node, applicationStore)
	if err != nil {
		log.Fatal(err)
	}
	pb.RegisterLockServiceServer(grpcServer, lockServer)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(listener); err != nil &&
		!errors.Is(err, grpc.ErrServerStopped) {
		log.Fatal(err)
	}

}

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
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("database url not found")
	}

	pool, err := storage.OpenPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	store := storage.NewPostgresStore(pool)
	transport, err := raft.NewGRPCTransport(cfg.PeerAddrs)
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()

	stateMachine := loggingStateMachine{}
	node, err := raft.NewNode(
		ctx,
		cfg, store,
		stateMachine,
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
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
	lock_service := api.LockServer{}
	pb.RegisterLockServiceServer(grpcServer, &lock_service)
	if err := grpcServer.Serve(listener); err != nil &&
		!errors.Is(err, grpc.ErrServerStopped) {
		log.Fatal(err)
	}

}

type loggingStateMachine struct{}

func (loggingStateMachine) Apply(
	ctx context.Context,
	entry storage.RaftLog,
) error {
	log.Printf("Applying Raft Log")
	return nil
}
func (loggingStateMachine) Snapshot(
	ctx context.Context,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []byte{}, nil
}
func (loggingStateMachine) Restore(
	ctx context.Context,
	data []byte,
) error {
	log.Printf("Applying Raft Log")
	return nil
}

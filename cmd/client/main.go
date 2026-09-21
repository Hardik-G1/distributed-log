package main

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg, err := config.ClientConfigLoad(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s, and address %s", cfg.ClientID, cfg.InitialServerAddr)

	conn, err := grpc.NewClient(cfg.InitialServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	lockClient := pb.NewLockServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
	defer cancel()
	request := &pb.AcquireLockRequest{
		ClientId:          "1",
		RequestId:         "1",
		ResourceId:        "1",
		ClientRequestedAt: time.Now().Unix(),
	}
	response, err := lockClient.AcquireLock(ctx, request)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s and %s", response.LockToken, response.LockStatus)
}

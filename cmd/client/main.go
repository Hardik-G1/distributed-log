package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	clientPkg "github.com/Hardik-G1/distributed-log/client"
	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/config"
)

func main() {
	cfg, err := config.ClientConfigLoad(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s, and address %s", cfg.ClientID, cfg.InitialServerAddr)
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()
	client, err := clientPkg.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	resource := "resource-1"

	response, err := client.AcquireLock(ctx, resource)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s and %s", response.LockToken, response.LockStatus.String())
	if response.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		log.Fatalf("lock was not approved %s", response.LockStatus.String())
	}
	lockToken := response.LockToken
	log.Printf("lock acquired %s", lockToken)
	appendResponse, err := client.AppendLog(
		ctx,
		resource,
		"test 1",
		lockToken,
	)
	if err != nil {
		log.Fatal(err)
	}
	if appendResponse.AppendStatus != pb.AppendStatus_APPEND_STATUS_COMMITTED {
		log.Fatalf("append failed %s", appendResponse.AppendStatus.String())
	}
	getResponse, err := client.GetLogData(
		ctx,
		resource,
		0,
		-1,
	)
	if err != nil {
		log.Fatal(err)
	}
	if getResponse.Status != pb.GetStatus_GET_STATUS_FETCHED {
		log.Fatalf("get failed %s", getResponse.Status.String())
	}
	log.Printf("application log %s", getResponse.Content)
	releaseResponse, err := client.ReleaseLock(
		ctx,
		resource,
		lockToken,
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("lock release status %s", releaseResponse.ReleaseStatus.String())
}

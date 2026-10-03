package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type endpoint struct {
	address string
	conn    *grpc.ClientConn
	client  pb.LockServiceClient
}

type Client struct {
	clientID       string
	retryInterval  time.Duration
	requestTimeout time.Duration
	endpoints      []endpoint
}

func New(cfg *config.ClientConfig) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("client config is nil")
	}
	if cfg.ClientID == "" {
		return nil, errors.New("client id is empty")
	}
	if cfg.InitialServerAddr == "" {
		return nil, errors.New("initial server address is empty")
	}
	addresses := make([]string, 0)
	addAddress := func(address string) {
		if address == "" {
			return
		}
		for _, existing := range addresses {
			if existing == address {
				return
			}
		}
		addresses = append(addresses, address)
	}
	addAddress(cfg.InitialServerAddr)
	for _, address := range cfg.DestinationAddrs {
		addAddress(address)
	}
	client := &Client{
		clientID:       cfg.ClientID,
		retryInterval:  cfg.RetryInterval,
		requestTimeout: cfg.RequestTimeout,
		endpoints:      make([]endpoint, 0, len(addresses)),
	}
	for _, address := range addresses {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("create connection to %s %w", address, err)
		}
		conn.Connect()
		client.endpoints = append(client.endpoints, endpoint{
			address: address,
			conn:    conn,
			client:  pb.NewLockServiceClient(conn),
		})
	}
	return client, nil
}

func (client *Client) Close() {
	for _, endpoint := range client.endpoints {
		_ = endpoint.conn.Close()
	}
}

func (client *Client) AcquireLock(
	ctx context.Context,
	resourceID string,
) (*pb.AcquireLockResponse, error) {
	request := &pb.AcquireLockRequest{
		ClientId:          client.clientID,
		RequestId:         newRequestID(client.clientID, "acquire"),
		ResourceId:        resourceID,
		ClientRequestedAt: time.Now().Unix(),
	}
	return retryCall(
		ctx,
		client,
		func(callCtx context.Context, server pb.LockServiceClient) (*pb.AcquireLockResponse, error) {
			return server.AcquireLock(callCtx, request)
		},
	)
}
func (client *Client) ReleaseLock(
	ctx context.Context,
	resourceID string,
	lockToken string,
) (*pb.ReleaseLockResponse, error) {
	request := &pb.ReleaseLockRequest{
		ClientId:          client.clientID,
		RequestId:         newRequestID(client.clientID, "release"),
		ResourceId:        resourceID,
		LockToken:         lockToken,
		ClientRequestedAt: time.Now().Unix(),
	}
	return retryCall(
		ctx,
		client,
		func(callCtx context.Context, server pb.LockServiceClient) (*pb.ReleaseLockResponse, error) {
			return server.ReleaseLock(callCtx, request)
		},
	)
}
func (client *Client) AppendLog(
	ctx context.Context,
	resourceID string,
	message string,
	lockToken string,
) (*pb.AppendLogResponse, error) {
	request := &pb.AppendLogRequest{
		ClientId:     client.clientID,
		RequestId:    newRequestID(client.clientID, "append"),
		ResourceId:   resourceID,
		Message:      message,
		LockToken:    lockToken,
		ClientSentAt: time.Now().Unix(),
	}
	return retryCall(
		ctx,
		client,
		func(callCtx context.Context, server pb.LockServiceClient) (*pb.AppendLogResponse, error) {
			return server.AppendLog(callCtx, request)
		},
	)
}
func (client *Client) GetLogData(
	ctx context.Context,
	resourceID string,
	position int64,
	length int64,
) (*pb.GetLogDataResponse, error) {
	request := &pb.GetLogDataRequest{
		ClientId:     client.clientID,
		RequestId:    newRequestID(client.clientID, "get"),
		ResourceId:   resourceID,
		LogPosition:  position,
		Length:       length,
		ClientSentAt: time.Now().Unix(),
	}
	return retryCall(
		ctx,
		client,
		func(callCtx context.Context, server pb.LockServiceClient) (*pb.GetLogDataResponse, error) {
			return server.GetLogData(callCtx, request)
		},
	)
}

func retryCall[T any](
	ctx context.Context,
	client *Client,
	call func(context.Context, pb.LockServiceClient) (T, error),
) (T, error) {
	var zero T
	var lastErr error

	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		for _, endpoint := range client.endpoints {
			callCtx, cancel := context.WithTimeout(ctx, client.requestTimeout)
			result, err := call(callCtx, endpoint.client)
			cancel()
			if err == nil {
				return result, nil
			}
			lastErr = fmt.Errorf(
				"server %s: %w",
				endpoint.address,
				err,
			)
		}
		if attempt+1 < maxAttempts {
			timer := time.NewTimer(client.retryInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return zero, ctx.Err()
			case <-timer.C:

			}
		}
	}
	if lastErr == nil {
		lastErr = errors.New("all servers failed")
	}
	return zero, lastErr
}
func newRequestID(
	clientID string,
	operation string,
) string {
	return fmt.Sprintf("%s-%s-%d", clientID, operation, time.Now().UnixNano())
}

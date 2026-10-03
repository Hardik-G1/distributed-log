package config

import (
	"errors"
	"flag"
	"time"
)

type ClientConfig struct {
	ClientID          string
	InitialServerAddr string
	DestinationAddrs  map[string]string
	RetryInterval     time.Duration
	RequestTimeout    time.Duration
}

func ClientConfigLoad(args []string) (*ClientConfig, error) {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	clientID := flags.String("client-id", "", "unique client id")
	serverAddr := flags.String("initial-server-addr", "localhost:5001", "grpc server addr")
	destinationAddrRaw := flags.String("destinations", "", "peer address group")
	retryInterval := flags.Duration("retry-interval", 100*time.Millisecond, "time after request is retried")
	requestTimeout := flags.Duration("request-timeout", 500*time.Millisecond, "request timeout after which we move stop waiting")

	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	peerAddrs, err := parsePeers(*destinationAddrRaw)
	if err != nil {
		return nil, err
	}
	if *clientID == "" {
		return nil, errors.New("client id is necessary")
	}
	if *retryInterval <= (0 * time.Millisecond) {
		return nil, errors.New("retry interval could not be less or equal to 0")
	}
	if *requestTimeout <= (0 * time.Millisecond) {
		return nil, errors.New("request timeout could not be less or equal to 0")
	}
	if *requestTimeout <= *retryInterval {
		return nil, errors.New("request timeout needs to be greater than retry interval")
	}
	return &ClientConfig{
		ClientID:          *clientID,
		InitialServerAddr: *serverAddr,
		DestinationAddrs:  peerAddrs,
		RetryInterval:     *retryInterval,
		RequestTimeout:    *requestTimeout,
	}, nil
}

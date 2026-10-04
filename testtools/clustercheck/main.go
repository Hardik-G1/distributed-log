package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type endpoint struct {
	address string
	conn    *grpc.ClientConn
	client  pb.LockServiceClient
}

type cluster struct {
	endpoints []endpoint
	preferred atomic.Int64
}

func newCluster(addresses string) (*cluster, error) {
	parts := strings.Split(addresses, ",")
	sort.Strings(parts)
	c := &cluster{}
	for _, address := range parts {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("connect %s: %w", address, err)
		}
		conn.Connect()
		c.endpoints = append(c.endpoints, endpoint{
			address: address,
			conn:    conn,
			client:  pb.NewLockServiceClient(conn),
		})
	}
	if len(c.endpoints) == 0 {
		return nil, errors.New("no endpoints supplied")
	}
	return c, nil
}

func (c *cluster) Close() {
	for _, endpoint := range c.endpoints {
		_ = endpoint.conn.Close()
	}
}

func (c *cluster) endpointOrder() []int {
	count := len(c.endpoints)
	if count == 0 {
		return nil
	}
	start := int(c.preferred.Load() % int64(count))
	order := make([]int, 0, count)
	for offset := 0; offset < count; offset++ {
		order = append(order, (start+offset)%count)
	}
	return order
}

func requestID(prefix string, n int64) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), n)
}

func (c *cluster) acquire(req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, string, error) {
	var lastErr error
	for _, endpointIndex := range c.endpointOrder() {
		endpoint := c.endpoints[endpointIndex]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		response, err := endpoint.client.AcquireLock(ctx, req)
		cancel()
		if err == nil {
			c.preferred.Store(int64(endpointIndex))
			return response, endpoint.address, nil
		}
		lastErr = fmt.Errorf("%s: %w", endpoint.address, err)
	}
	return nil, "", lastErr
}

func (c *cluster) release(req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, string, error) {
	var lastErr error
	for _, endpointIndex := range c.endpointOrder() {
		endpoint := c.endpoints[endpointIndex]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		response, err := endpoint.client.ReleaseLock(ctx, req)
		cancel()
		if err == nil {
			c.preferred.Store(int64(endpointIndex))
			return response, endpoint.address, nil
		}
		lastErr = fmt.Errorf("%s: %w", endpoint.address, err)
	}
	return nil, "", lastErr
}

func (c *cluster) append(req *pb.AppendLogRequest) (*pb.AppendLogResponse, string, error) {
	var lastErr error
	for _, endpointIndex := range c.endpointOrder() {
		endpoint := c.endpoints[endpointIndex]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		response, err := endpoint.client.AppendLog(ctx, req)
		cancel()
		if err == nil {
			c.preferred.Store(int64(endpointIndex))
			return response, endpoint.address, nil
		}
		lastErr = fmt.Errorf("%s: %w", endpoint.address, err)
	}
	return nil, "", lastErr
}

func (c *cluster) get(req *pb.GetLogDataRequest) (*pb.GetLogDataResponse, string, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		for _, endpointIndex := range c.endpointOrder() {
			endpoint := c.endpoints[endpointIndex]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			response, err := endpoint.client.GetLogData(ctx, req)
			cancel()
			if err == nil {
				c.preferred.Store(int64(endpointIndex))
				return response, endpoint.address, nil
			}
			lastErr = fmt.Errorf("%s: %w", endpoint.address, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, "", lastErr
}

func runSanity(c *cluster, resource string) error {
	clientID := "sanity-client"
	lockRequest := &pb.AcquireLockRequest{
		ClientId:          clientID,
		RequestId:         requestID("sanity-acquire", 1),
		ResourceId:        resource,
		ClientRequestedAt: time.Now().Unix(),
	}
	lock, leader, err := c.acquire(lockRequest)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	if lock.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		return fmt.Errorf("acquire status: %s", lock.LockStatus.String())
	}
	log.Printf("sanity acquire leader=%s status=%s", leader, lock.LockStatus.String())

	// Replaying the exact request must return the same response.
	duplicate, _, err := c.acquire(lockRequest)
	if err != nil {
		return fmt.Errorf("duplicate acquire: %w", err)
	}
	if duplicate.LockToken != lock.LockToken || duplicate.Expiry != lock.Expiry {
		return errors.New("duplicate acquire returned a different response")
	}
	invalidAcquire, _, err := c.acquire(&pb.AcquireLockRequest{
		ClientId: "sanity-invalid-client", RequestId: requestID("sanity-invalid-acquire", 1),
		ResourceId: "missing-resource", ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("invalid acquire: %w", err)
	}
	if invalidAcquire.LockStatus != pb.LockStatus_LOCK_STATUS_INVALID_REQUEST {
		return fmt.Errorf("invalid acquire status: %s", invalidAcquire.LockStatus.String())
	}
	busyAcquire, _, err := c.acquire(&pb.AcquireLockRequest{
		ClientId: "sanity-contender", RequestId: requestID("sanity-busy-acquire", 1),
		ResourceId: resource, ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("busy acquire: %w", err)
	}
	if busyAcquire.LockStatus != pb.LockStatus_LOCK_STATUS_BUSY {
		return fmt.Errorf("busy acquire status: %s", busyAcquire.LockStatus.String())
	}
	staleAppend, _, err := c.append(&pb.AppendLogRequest{
		ClientId: clientID, RequestId: requestID("sanity-stale-append", 1),
		ResourceId: resource, Message: "must-not-appear", LockToken: "wrong-token",
		ClientSentAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("stale append: %w", err)
	}
	if staleAppend.AppendStatus != pb.AppendStatus_APPEND_STATUS_STALE_TOKEN {
		return fmt.Errorf("stale append status: %s", staleAppend.AppendStatus.String())
	}
	staleRelease, _, err := c.release(&pb.ReleaseLockRequest{
		ClientId: clientID, RequestId: requestID("sanity-stale-release", 1),
		ResourceId: resource, LockToken: "wrong-token", ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("stale release: %w", err)
	}
	if staleRelease.ReleaseStatus != pb.ReleaseStatus_RELEASE_STATUS_STALE_TOKEN {
		return fmt.Errorf("stale release status: %s", staleRelease.ReleaseStatus.String())
	}

	appendRequest := &pb.AppendLogRequest{
		ClientId:     clientID,
		RequestId:    requestID("sanity-append", 1),
		ResourceId:   resource,
		Message:      "sanity-message",
		LockToken:    lock.LockToken,
		ClientSentAt: time.Now().Unix(),
	}
	appended, _, err := c.append(appendRequest)
	if err != nil {
		return fmt.Errorf("append: %w", err)
	}
	if appended.AppendStatus != pb.AppendStatus_APPEND_STATUS_COMMITTED {
		return fmt.Errorf("append status: %s", appended.AppendStatus.String())
	}
	duplicateAppend, _, err := c.append(appendRequest)
	if err != nil {
		return fmt.Errorf("duplicate append: %w", err)
	}
	if duplicateAppend.AppendStatus != pb.AppendStatus_APPEND_STATUS_COMMITTED {
		return fmt.Errorf("duplicate append status: %s", duplicateAppend.AppendStatus.String())
	}

	getResponse, _, err := c.get(&pb.GetLogDataRequest{
		ClientId:     clientID,
		RequestId:    requestID("sanity-get", 1),
		ResourceId:   resource,
		LogPosition:  0,
		Length:       -1,
		ClientSentAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if getResponse.Status != pb.GetStatus_GET_STATUS_FETCHED || getResponse.Content != "sanity-message" {
		return fmt.Errorf("get result: status=%s content=%q", getResponse.Status.String(), getResponse.Content)
	}
	invalidResource, _, err := c.get(&pb.GetLogDataRequest{
		ClientId: clientID, RequestId: requestID("sanity-get-missing", 1),
		ResourceId: "missing-resource", LogPosition: 0, Length: -1,
	})
	if err != nil || invalidResource.Status != pb.GetStatus_GET_STATUS_INVALID_RESOURCE {
		return fmt.Errorf("invalid resource read: response=%v error=%v", invalidResource, err)
	}
	invalidPosition, _, err := c.get(&pb.GetLogDataRequest{
		ClientId: clientID, RequestId: requestID("sanity-get-position", 1),
		ResourceId: resource, LogPosition: -1, Length: -1,
	})
	if err != nil || invalidPosition.Status != pb.GetStatus_GET_STATUS_INVALID_POSITION {
		return fmt.Errorf("invalid position read: response=%v error=%v", invalidPosition, err)
	}
	invalidLength, _, err := c.get(&pb.GetLogDataRequest{
		ClientId: clientID, RequestId: requestID("sanity-get-length", 1),
		ResourceId: resource, LogPosition: 0, Length: -2,
	})
	if err != nil || invalidLength.Status != pb.GetStatus_GET_STATUS_INVALID_LENGTH {
		return fmt.Errorf("invalid length read: response=%v error=%v", invalidLength, err)
	}

	releaseRequest := &pb.ReleaseLockRequest{
		ClientId:          clientID,
		RequestId:         requestID("sanity-release", 1),
		ResourceId:        resource,
		LockToken:         lock.LockToken,
		ClientRequestedAt: time.Now().Unix(),
	}
	released, _, err := c.release(releaseRequest)
	if err != nil {
		return fmt.Errorf("release: %w", err)
	}
	if released.ReleaseStatus != pb.ReleaseStatus_RELEASE_STATUS_RELEASED {
		return fmt.Errorf("release status: %s", released.ReleaseStatus.String())
	}
	duplicateRelease, _, err := c.release(releaseRequest)
	if err != nil {
		return fmt.Errorf("duplicate release: %w", err)
	}
	if duplicateRelease.ReleaseStatus != pb.ReleaseStatus_RELEASE_STATUS_RELEASED {
		return fmt.Errorf("duplicate release status: %s", duplicateRelease.ReleaseStatus.String())
	}
	log.Printf("sanity workflow passed")
	return nil
}

func runFailover(c *cluster, servers *managedServers, resource string) error {
	if servers == nil {
		return errors.New("failover mode requires --manage-servers")
	}
	firstClient := "failover-before-client"
	firstLock, oldLeader, err := c.acquire(&pb.AcquireLockRequest{
		ClientId:          firstClient,
		RequestId:         requestID("failover-before-acquire", 1),
		ResourceId:        resource,
		ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("acquire before failover: %w", err)
	}
	if firstLock.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		return fmt.Errorf("acquire before failover returned %s", firstLock.LockStatus.String())
	}
	if _, _, err := c.release(&pb.ReleaseLockRequest{
		ClientId:          firstClient,
		RequestId:         requestID("failover-before-release", 1),
		ResourceId:        resource,
		LockToken:         firstLock.LockToken,
		ClientRequestedAt: time.Now().Unix(),
	}); err != nil {
		return fmt.Errorf("release before failover: %w", err)
	}
	if err := servers.KillAddress(oldLeader); err != nil {
		return fmt.Errorf("kill old leader: %w", err)
	}
	log.Printf("failover killed leader=%s", oldLeader)
	time.Sleep(3 * time.Second)

	secondClient := "failover-after-client"
	secondLock, newLeader, err := c.acquire(&pb.AcquireLockRequest{
		ClientId:          secondClient,
		RequestId:         requestID("failover-after-acquire", 1),
		ResourceId:        resource,
		ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("acquire after failover: %w", err)
	}
	if secondLock.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		return fmt.Errorf("acquire after failover returned %s", secondLock.LockStatus.String())
	}
	if newLeader == oldLeader {
		return fmt.Errorf("leader did not change from %s", oldLeader)
	}
	message := fmt.Sprintf("failover-message-%d", time.Now().UnixNano())
	appendResponse, _, err := c.append(&pb.AppendLogRequest{
		ClientId:     secondClient,
		RequestId:    requestID("failover-append", 1),
		ResourceId:   resource,
		Message:      message,
		LockToken:    secondLock.LockToken,
		ClientSentAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("append after failover: %w", err)
	}
	if appendResponse.AppendStatus != pb.AppendStatus_APPEND_STATUS_COMMITTED {
		return fmt.Errorf("append after failover returned %s", appendResponse.AppendStatus.String())
	}
	readResponse, _, err := c.get(&pb.GetLogDataRequest{
		ClientId:     secondClient,
		RequestId:    requestID("failover-get", 1),
		ResourceId:   resource,
		LogPosition:  0,
		Length:       -1,
		ClientSentAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("read after failover: %w", err)
	}
	if readResponse.Status != pb.GetStatus_GET_STATUS_FETCHED || !strings.Contains(readResponse.Content, message) {
		return fmt.Errorf("read after failover returned status=%s", readResponse.Status.String())
	}
	log.Printf("failover correctness=PASS old_leader=%s new_leader=%s", oldLeader, newLeader)
	return nil
}

func appendAndRelease(
	c *cluster,
	clientID string,
	resource string,
	message string,
) error {
	lock, _, err := c.acquire(&pb.AcquireLockRequest{
		ClientId:          clientID,
		RequestId:         requestID(clientID+"-acquire", 1),
		ResourceId:        resource,
		ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("acquire for %s: %w", clientID, err)
	}
	if lock.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		return fmt.Errorf("acquire for %s returned %s", clientID, lock.LockStatus.String())
	}
	appended, _, err := c.append(&pb.AppendLogRequest{
		ClientId:     clientID,
		RequestId:    requestID(clientID+"-append", 1),
		ResourceId:   resource,
		Message:      message,
		LockToken:    lock.LockToken,
		ClientSentAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("append for %s: %w", clientID, err)
	}
	if appended.AppendStatus != pb.AppendStatus_APPEND_STATUS_COMMITTED {
		return fmt.Errorf("append for %s returned %s", clientID, appended.AppendStatus.String())
	}
	released, _, err := c.release(&pb.ReleaseLockRequest{
		ClientId:          clientID,
		RequestId:         requestID(clientID+"-release", 1),
		ResourceId:        resource,
		LockToken:         lock.LockToken,
		ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("release for %s: %w", clientID, err)
	}
	if released.ReleaseStatus != pb.ReleaseStatus_RELEASE_STATUS_RELEASED {
		return fmt.Errorf("release for %s returned %s", clientID, released.ReleaseStatus.String())
	}
	return nil
}

func runFollowerRecovery(c *cluster, servers *managedServers, resource string) error {
	if servers == nil || len(servers.servers) != 3 {
		return errors.New("follower-recovery mode requires a managed three-node cluster")
	}
	discoveryClient := "follower-recovery-discovery"
	discoveryLock, leader, err := c.acquire(&pb.AcquireLockRequest{
		ClientId:          discoveryClient,
		RequestId:         requestID("follower-recovery-discovery", 1),
		ResourceId:        resource,
		ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("discover leader: %w", err)
	}
	if discoveryLock.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		return fmt.Errorf("leader discovery returned %s", discoveryLock.LockStatus.String())
	}
	if _, _, err := c.release(&pb.ReleaseLockRequest{
		ClientId:          discoveryClient,
		RequestId:         requestID("follower-recovery-discovery-release", 1),
		ResourceId:        resource,
		LockToken:         discoveryLock.LockToken,
		ClientRequestedAt: time.Now().Unix(),
	}); err != nil {
		return fmt.Errorf("release discovery lock: %w", err)
	}

	followers := make([]string, 0, 2)
	for _, server := range servers.servers {
		if server.address != leader {
			followers = append(followers, server.address)
		}
	}
	if len(followers) != 2 {
		return fmt.Errorf("expected two followers for leader %s, got %v", leader, followers)
	}

	if err := servers.KillAddress(followers[0]); err != nil {
		return fmt.Errorf("stop first follower: %w", err)
	}
	messageBeforeRestart := fmt.Sprintf("follower-down-%d", time.Now().UnixNano())
	if err := appendAndRelease(c, "follower-down-client", resource, messageBeforeRestart); err != nil {
		return err
	}

	if err := servers.RestartAddress(followers[0]); err != nil {
		return fmt.Errorf("restart first follower: %w", err)
	}
	// Give replication time to transfer the suffix written while this follower
	// was unavailable before making it the leader's only quorum partner.
	time.Sleep(2 * time.Second)
	if err := servers.KillAddress(followers[1]); err != nil {
		return fmt.Errorf("stop second follower: %w", err)
	}

	messageAfterRestart := fmt.Sprintf("follower-rejoined-%d", time.Now().UnixNano())
	if err := appendAndRelease(c, "follower-rejoined-client", resource, messageAfterRestart); err != nil {
		return fmt.Errorf("restarted follower did not participate in quorum: %w", err)
	}
	readResponse, _, err := c.get(&pb.GetLogDataRequest{
		ClientId:     "follower-recovery-reader",
		RequestId:    requestID("follower-recovery-read", 1),
		ResourceId:   resource,
		LogPosition:  0,
		Length:       -1,
		ClientSentAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("read after follower recovery: %w", err)
	}
	if readResponse.Status != pb.GetStatus_GET_STATUS_FETCHED ||
		!strings.Contains(readResponse.Content, messageBeforeRestart) ||
		!strings.Contains(readResponse.Content, messageAfterRestart) {
		return fmt.Errorf("follower recovery content mismatch status=%s content=%q", readResponse.Status.String(), readResponse.Content)
	}
	log.Printf(
		"follower-recovery correctness=PASS leader=%s restarted=%s removed=%s",
		leader, followers[0], followers[1],
	)
	return nil
}

func runRestartRecovery(
	c *cluster,
	servers *managedServers,
	prefix string,
	clients int,
	messages int,
	resources int,
	concurrency int,
	lockBatch int,
	timeout time.Duration,
) error {
	if servers == nil {
		return errors.New("restart-recovery mode requires --manage-servers")
	}
	if err := runMulti(c, prefix, clients, messages, resources, concurrency, lockBatch, timeout); err != nil {
		return err
	}
	// Snapshot creation is periodic. Wait for the next check and require a real
	// snapshot so this test exercises snapshot plus WAL-tail recovery.
	time.Sleep(2 * time.Second)
	if !servers.AllHaveSnapshots() {
		return errors.New("restart-recovery did not create a snapshot; lower --snapshot-threshold or increase the workload")
	}
	if err := servers.RestartAll(); err != nil {
		return fmt.Errorf("restart cluster: %w", err)
	}
	correctness, err := validateWorkload(
		c,
		prefix,
		expectedWorkload(prefix, clients, messages, resources, lockBatch),
		timeout,
	)
	if err != nil {
		return fmt.Errorf("validate after full restart: %w", err)
	}
	log.Printf("restart-recovery correctness=%s", correctness)
	return nil
}

func runNoQuorum(c *cluster, servers *managedServers, resource string) error {
	if servers == nil || len(servers.servers) != 3 {
		return errors.New("no-quorum mode requires a managed three-node cluster")
	}
	discoveryClient := "no-quorum-discovery"
	discoveryLock, leader, err := c.acquire(&pb.AcquireLockRequest{
		ClientId:          discoveryClient,
		RequestId:         requestID("no-quorum-discovery", 1),
		ResourceId:        resource,
		ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("discover leader: %w", err)
	}
	if discoveryLock.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		return fmt.Errorf("leader discovery returned %s", discoveryLock.LockStatus.String())
	}
	if _, _, err := c.release(&pb.ReleaseLockRequest{
		ClientId:          discoveryClient,
		RequestId:         requestID("no-quorum-discovery-release", 1),
		ResourceId:        resource,
		LockToken:         discoveryLock.LockToken,
		ClientRequestedAt: time.Now().Unix(),
	}); err != nil {
		return fmt.Errorf("release discovery lock: %w", err)
	}
	for _, server := range servers.servers {
		if server.address == leader {
			continue
		}
		if err := servers.KillAddress(server.address); err != nil {
			return err
		}
	}
	time.Sleep(500 * time.Millisecond)
	response, endpoint, err := c.acquire(&pb.AcquireLockRequest{
		ClientId: "no-quorum-client", RequestId: requestID("no-quorum", 1),
		ResourceId: resource, ClientRequestedAt: time.Now().Unix(),
	})
	if err == nil {
		return fmt.Errorf("write unexpectedly succeeded without quorum through %s: %v", endpoint, response)
	}
	log.Printf("no-quorum correctness=PASS isolated_leader=%s write rejected: %v", leader, err)
	return nil
}

func runStress(c *cluster, resource string, requests, concurrency int) error {
	lock, leader, err := c.acquire(&pb.AcquireLockRequest{
		ClientId:          "stress-lock-client",
		RequestId:         requestID("stress-acquire", 1),
		ResourceId:        resource,
		ClientRequestedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("stress acquire: %w", err)
	}
	if lock.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		return fmt.Errorf("stress acquire status: %s", lock.LockStatus.String())
	}
	log.Printf("stress lock leader=%s expiry=%d", leader, lock.Expiry)

	var success atomic.Int64
	var failed atomic.Int64
	var busy atomic.Int64
	var expired atomic.Int64
	latencies := make(chan time.Duration, requests)
	var wg sync.WaitGroup
	start := time.Now()
	jobs := make(chan int, requests)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				requestStarted := time.Now()
				response, _, err := c.append(&pb.AppendLogRequest{
					ClientId:     "stress-lock-client",
					RequestId:    requestID("stress-append", int64(n)),
					ResourceId:   resource,
					Message:      fmt.Sprintf("stress-%d", n),
					LockToken:    lock.LockToken,
					ClientSentAt: time.Now().Unix(),
				})
				latencies <- time.Since(requestStarted)
				if err != nil {
					failed.Add(1)
					continue
				}
				switch response.AppendStatus {
				case pb.AppendStatus_APPEND_STATUS_COMMITTED:
					success.Add(1)
				case pb.AppendStatus_APPEND_STATUS_EXPIRED_LOCK:
					expired.Add(1)
				default:
					busy.Add(1)
				}
			}
		}()
	}
	for i := 0; i < requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(latencies)
	duration := time.Since(start)
	latencySamples := make([]time.Duration, 0, requests)
	for latency := range latencies {
		latencySamples = append(latencySamples, latency)
	}
	sort.Slice(latencySamples, func(i, j int) bool {
		return latencySamples[i] < latencySamples[j]
	})
	log.Printf("stress requests=%d concurrency=%d duration=%s success=%d failed=%d expired=%d other-status=%d throughput=%.2f successful_ops_per_sec=%.2f p50=%s p95=%s p99=%s max=%s",
		requests, concurrency, duration, success.Load(), failed.Load(), expired.Load(), busy.Load(),
		float64(requests)/duration.Seconds(), float64(success.Load())/duration.Seconds(),
		percentile(latencySamples, 0.50), percentile(latencySamples, 0.95),
		percentile(latencySamples, 0.99), percentile(latencySamples, 1.00))
	return nil
}

func runVerify(c *cluster, resource string, expectedCount int) error {
	if expectedCount < 0 {
		return errors.New("expected count cannot be negative")
	}
	position := int64(0)
	for {
		response, leader, err := c.get(&pb.GetLogDataRequest{
			ClientId:     "verify-client",
			RequestId:    requestID("verify-get", position),
			ResourceId:   resource,
			LogPosition:  position,
			Length:       1000,
			ClientSentAt: time.Now().Unix(),
		})
		if err != nil {
			return fmt.Errorf("verify %s at %d: %w", resource, position, err)
		}
		if response.Status != pb.GetStatus_GET_STATUS_FETCHED {
			return fmt.Errorf("verify %s returned %s", resource, response.Status.String())
		}
		if response.Content == "" {
			if position != int64(expectedCount) {
				return fmt.Errorf("verify %s counted %d messages, expected %d", resource, position, expectedCount)
			}
			log.Printf("verify leader=%s resource=%s message_count=%d correctness=PASS", leader, resource, position)
			return nil
		}
		position += int64(len(strings.Split(response.Content, "\n")))
		if position > int64(expectedCount) {
			return fmt.Errorf("verify %s exceeded expected count: %d > %d", resource, position, expectedCount)
		}
	}
}

func workloadMessage(client, sequence int) string {
	return fmt.Sprintf("load-client-%06d-message-%08d", client, sequence)
}

func workloadResource(
	prefix string,
	client int,
	sequence int,
	resources int,
	lockBatch int,
) string {
	cycle := sequence / lockBatch
	// SplitMix64's finalizer gives sequential logical client IDs a stable,
	// well-distributed resource assignment, including when the resource count
	// has factors in common with common linear-congruential multipliers.
	value := uint64(client)<<32 | uint64(uint32(cycle))
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	resourceIndex := int(value % uint64(resources))
	return fmt.Sprintf("%s-%04d", prefix, resourceIndex)
}

func expectedWorkload(
	prefix string,
	clients int,
	messages int,
	resources int,
	lockBatch int,
) map[string]map[string]struct{} {
	expected := make(map[string]map[string]struct{}, resources)
	for resourceIndex := 0; resourceIndex < resources; resourceIndex++ {
		expected[fmt.Sprintf("%s-%04d", prefix, resourceIndex)] = make(map[string]struct{})
	}
	for client := 0; client < clients; client++ {
		for sequence := 0; sequence < messages; sequence++ {
			resource := workloadResource(prefix, client, sequence, resources, lockBatch)
			expected[resource][workloadMessage(client, sequence)] = struct{}{}
		}
	}
	return expected
}

func acquireWorkloadLock(
	c *cluster,
	ctx context.Context,
	clientID string,
	resource string,
	cycle int,
) (string, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		response, _, err := c.acquire(&pb.AcquireLockRequest{
			ClientId:          clientID,
			RequestId:         fmt.Sprintf("load-acquire-%s-%d-%d", clientID, cycle, attempt),
			ResourceId:        resource,
			ClientRequestedAt: time.Now().Unix(),
		})
		if err == nil && response.LockStatus == pb.LockStatus_LOCK_STATUS_APPROVED {
			return response.LockToken, nil
		}
		if err == nil && response.LockStatus != pb.LockStatus_LOCK_STATUS_BUSY {
			return "", fmt.Errorf("acquire %s: %s", resource, response.LockStatus.String())
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}

func runMulti(
	c *cluster,
	prefix string,
	clients int,
	messages int,
	resources int,
	concurrency int,
	lockBatch int,
	timeout time.Duration,
) error {
	if clients <= 0 || messages <= 0 || resources <= 0 || concurrency <= 0 || lockBatch <= 0 {
		return errors.New("clients, messages, resources, concurrency, and lock-batch must be positive")
	}
	if timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	total := clients * messages
	if total/clients != messages {
		return errors.New("workload size overflows int")
	}
	expected := expectedWorkload(prefix, clients, messages, resources, lockBatch)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	jobs := make(chan int)
	latencies := make(chan time.Duration, min(total, 65536))
	latencyResult := make(chan []time.Duration, 1)
	go func() {
		samples := make([]time.Duration, 0, total)
		for latency := range latencies {
			samples = append(samples, latency)
		}
		latencyResult <- samples
	}()
	var success atomic.Int64
	var failed atomic.Int64
	var releaseFailures atomic.Int64
	var attempts atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	workerCount := concurrency
	if workerCount > clients {
		workerCount = clients
	}
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for client := range jobs {
				// Include the workload prefix so repeated runs against the same
				// databases do not collide with durable idempotency records from
				// an earlier run.
				clientID := fmt.Sprintf("%s-client-%06d", prefix, client)
				resource := ""
				lockToken := ""
				lockCycle := 0
				appendRequestID := ""
				appendRequestSequence := -1
				for sequence := 0; sequence < messages; {
					if lockToken == "" || sequence%lockBatch == 0 {
						if lockToken != "" {
							response, _, err := c.release(&pb.ReleaseLockRequest{
								ClientId:          clientID,
								RequestId:         fmt.Sprintf("load-release-%s-%d", clientID, sequence),
								ResourceId:        resource,
								LockToken:         lockToken,
								ClientRequestedAt: time.Now().Unix(),
							})
							if err != nil || response.ReleaseStatus != pb.ReleaseStatus_RELEASE_STATUS_RELEASED {
								releaseFailures.Add(1)
							}
							lockToken = ""
						}
						resource = workloadResource(
							prefix, client, sequence, resources, lockBatch,
						)
						var err error
						lockToken, err = acquireWorkloadLock(c, ctx, clientID, resource, lockCycle)
						lockCycle++
						if err != nil {
							failed.Add(int64(messages - sequence))
							break
						}
					}

					message := workloadMessage(client, sequence)
					if appendRequestSequence != sequence {
						appendRequestSequence = sequence
						appendRequestID = fmt.Sprintf("load-append-%s-%08d", clientID, sequence)
					}
					requestStarted := time.Now()
					attempts.Add(1)
					response, _, err := c.append(&pb.AppendLogRequest{
						ClientId:     clientID,
						RequestId:    appendRequestID,
						ResourceId:   resource,
						Message:      message,
						LockToken:    lockToken,
						ClientSentAt: time.Now().Unix(),
					})
					latencies <- time.Since(requestStarted)
					if err != nil {
						failed.Add(1)
						lockToken = ""
						continue
					}
					if response.AppendStatus == pb.AppendStatus_APPEND_STATUS_COMMITTED {
						success.Add(1)
						sequence++
						continue
					}
					if response.AppendStatus == pb.AppendStatus_APPEND_STATUS_EXPIRED_LOCK {
						lockToken = ""
						// An expired-lock response is a completed negative result for
						// this request ID. Use a new ID after reacquiring the lock;
						// network/time-out errors above intentionally reuse the ID.
						appendRequestID = fmt.Sprintf(
							"load-append-%s-%08d-%d",
							clientID,
							sequence,
							time.Now().UnixNano(),
						)
						continue
					}
					failed.Add(1)
					lockToken = ""
				}
				if lockToken != "" {
					response, _, err := c.release(&pb.ReleaseLockRequest{
						ClientId:          clientID,
						RequestId:         fmt.Sprintf("load-release-%s-final", clientID),
						ResourceId:        resource,
						LockToken:         lockToken,
						ClientRequestedAt: time.Now().Unix(),
					})
					if err != nil || response.ReleaseStatus != pb.ReleaseStatus_RELEASE_STATUS_RELEASED {
						releaseFailures.Add(1)
					}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for client := 0; client < clients; client++ {
			select {
			case jobs <- client:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	close(latencies)

	duration := time.Since(start)
	samples := <-latencyResult
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	log.Printf("multi workload finished clients=%d messages_per_client=%d resources=%d requested=%d attempts=%d concurrency=%d lock_batch=%d duration=%s committed=%d failed=%d release_failures=%d throughput=%.2f p50=%s p95=%s p99=%s max=%s",
		clients, messages, resources, total, attempts.Load(), concurrency, lockBatch, duration,
		success.Load(), failed.Load(), releaseFailures.Load(),
		float64(success.Load())/duration.Seconds(), percentile(samples, 0.50),
		percentile(samples, 0.95), percentile(samples, 0.99), percentile(samples, 1.00))
	correctness, err := validateWorkload(c, prefix, expected, timeout)
	if err != nil {
		return err
	}
	log.Printf("multi clients=%d messages_per_client=%d resources=%d requested=%d attempts=%d concurrency=%d lock_batch=%d duration=%s committed=%d failed=%d release_failures=%d throughput=%.2f p50=%s p95=%s p99=%s max=%s correctness=%s",
		clients, messages, resources, total, attempts.Load(), concurrency, lockBatch, duration,
		success.Load(), failed.Load(), releaseFailures.Load(),
		float64(success.Load())/duration.Seconds(), percentile(samples, 0.50),
		percentile(samples, 0.95), percentile(samples, 0.99), percentile(samples, 1.00), correctness)
	if success.Load() != int64(total) {
		return fmt.Errorf("committed %d of %d requested messages", success.Load(), total)
	}
	if !strings.HasPrefix(correctness, "PASS") {
		return errors.New("application-log correctness failed")
	}
	return nil
}

func validateWorkload(
	c *cluster,
	prefix string,
	expected map[string]map[string]struct{},
	timeout time.Duration,
) (string, error) {
	seenTotal := 0
	for resource, wanted := range expected {
		seen := make(map[string]struct{}, len(wanted))
		position := int64(0)
		for {
			response, _, err := c.get(&pb.GetLogDataRequest{
				ClientId:     "load-validator",
				RequestId:    requestID("load-get", position),
				ResourceId:   resource,
				LogPosition:  position,
				Length:       1000,
				ClientSentAt: time.Now().Unix(),
			})
			if err != nil {
				return "FAIL", fmt.Errorf("validate %s at %d: %w", resource, position, err)
			}
			if response.Status != pb.GetStatus_GET_STATUS_FETCHED {
				return "FAIL", fmt.Errorf("validate %s returned %s", resource, response.Status.String())
			}
			if response.Content == "" {
				break
			}
			for _, line := range strings.Split(response.Content, "\n") {
				if _, ok := wanted[line]; !ok {
					return "FAIL", fmt.Errorf("unexpected message %q in %s", line, resource)
				}
				if _, duplicate := seen[line]; duplicate {
					return "FAIL", fmt.Errorf("duplicate message %q in %s", line, resource)
				}
				seen[line] = struct{}{}
			}
			position += int64(len(strings.Split(response.Content, "\n")))
		}
		if len(seen) != len(wanted) {
			return "FAIL", fmt.Errorf("resource %s has %d messages, expected %d", resource, len(seen), len(wanted))
		}
		seenTotal += len(seen)
	}
	return fmt.Sprintf("PASS (%d unique messages across %d resources)", seenTotal, len(expected)), nil
}

func percentile(samples []time.Duration, fraction float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	index := int(float64(len(samples)-1) * fraction)
	return samples[index]
}

type managedServer struct {
	address       string
	executable    string
	workDirectory string
	args          []string
	stdoutPath    string
	stderrPath    string
	cmd           *exec.Cmd
	stdout        *os.File
	stderr        *os.File
}

func (server *managedServer) start() error {
	if server.cmd != nil {
		return fmt.Errorf("server %s is already running", server.address)
	}
	stdout, err := os.OpenFile(server.stdoutPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open stdout log for %s: %w", server.address, err)
	}
	stderr, err := os.OpenFile(server.stderrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = stdout.Close()
		return fmt.Errorf("open stderr log for %s: %w", server.address, err)
	}
	cmd := exec.Command(server.executable, server.args...)
	cmd.Dir = server.workDirectory
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return fmt.Errorf("start %s: %w", server.address, err)
	}
	server.cmd = cmd
	server.stdout = stdout
	server.stderr = stderr
	return nil
}

func (server *managedServer) stop() {
	if server.cmd != nil {
		if server.cmd.Process != nil {
			_ = server.cmd.Process.Kill()
		}
		_ = server.cmd.Wait()
		server.cmd = nil
	}
	if server.stdout != nil {
		_ = server.stdout.Close()
		server.stdout = nil
	}
	if server.stderr != nil {
		_ = server.stderr.Close()
		server.stderr = nil
	}
}

func (servers *managedServers) KillAddress(address string) error {
	for _, server := range servers.servers {
		if server.address != address {
			continue
		}
		if server.cmd == nil || server.cmd.Process == nil {
			return fmt.Errorf("server %s has no process", address)
		}
		server.stop()
		return nil
	}
	return fmt.Errorf("managed server %s was not found", address)
}

func (servers *managedServers) RestartAddress(address string) error {
	for _, server := range servers.servers {
		if server.address != address {
			continue
		}
		server.stop()
		if err := server.start(); err != nil {
			return err
		}
		return waitForPort(address, 30*time.Second)
	}
	return fmt.Errorf("managed server %s was not found", address)
}

type managedServers struct {
	servers []*managedServer
}

func (servers *managedServers) Stop() {
	if servers == nil {
		return
	}
	for _, server := range servers.servers {
		server.stop()
	}
}

func (servers *managedServers) RestartAll() error {
	servers.Stop()
	for _, server := range servers.servers {
		if err := server.start(); err != nil {
			servers.Stop()
			return err
		}
	}
	for _, server := range servers.servers {
		if err := waitForPort(server.address, 30*time.Second); err != nil {
			servers.Stop()
			return err
		}
	}
	time.Sleep(2 * time.Second)
	return nil
}

func (servers *managedServers) AllHaveSnapshots() bool {
	for _, server := range servers.servers {
		matches, err := filepath.Glob(filepath.Join(server.workDirectory, "data", "snapshot", "*.bin"))
		if err != nil || len(matches) == 0 {
			return false
		}
	}
	return true
}

func waitForPort(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", address)
}

func startManagedServers(
	serverExecutable string,
	workRoot string,
	snapshotThreshold int,
	proposalBatchWait time.Duration,
	proposalBatchSize int,
	proposalBatchMax int,
) (*managedServers, error) {
	serverExecutable, err := filepath.Abs(serverExecutable)
	if err != nil {
		return nil, fmt.Errorf("resolve server executable: %w", err)
	}
	if _, err := os.Stat(serverExecutable); err != nil {
		return nil, fmt.Errorf("server executable %s: %w", serverExecutable, err)
	}
	workRoot, err = filepath.Abs(workRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve server work root: %w", err)
	}

	type nodeSpec struct {
		id      string
		address string
		peers   string
	}
	specs := []nodeSpec{
		{id: "node-1", address: "localhost:5001", peers: "node-2=localhost:5002,node-3=localhost:5003"},
		{id: "node-2", address: "localhost:5002", peers: "node-1=localhost:5001,node-3=localhost:5003"},
		{id: "node-3", address: "localhost:5003", peers: "node-1=localhost:5001,node-2=localhost:5002"},
	}
	for _, spec := range specs {
		conn, err := net.DialTimeout("tcp", spec.address, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil, fmt.Errorf("%s is already in use", spec.address)
		}
	}

	managed := &managedServers{}
	cleanup := true
	defer func() {
		if cleanup {
			managed.Stop()
		}
	}()
	for index, spec := range specs {
		workDirectory := filepath.Join(workRoot, fmt.Sprintf("node%d", index+1))
		if err := os.MkdirAll(workDirectory, 0o700); err != nil {
			return nil, fmt.Errorf("create work directory: %w", err)
		}
		args := []string{
			"--node-id=" + spec.id,
			"--listen-addr=" + spec.address,
			"--peers=" + spec.peers,
			"--heartbeat=100ms",
			"--election-timeout=800ms",
			fmt.Sprintf("--snapshot-threshold=%d", snapshotThreshold),
			"--snapshot-directory=data/snapshot",
			"--raft-data-directory=data/raft",
			"--app-data-directory=data/application",
			"--proposal-batch-wait=" + proposalBatchWait.String(),
			fmt.Sprintf("--proposal-batch-size=%d", proposalBatchSize),
			fmt.Sprintf("--proposal-batch-max=%d", proposalBatchMax),
		}
		server := &managedServer{
			address:       spec.address,
			executable:    serverExecutable,
			workDirectory: workDirectory,
			args:          args,
			stdoutPath:    filepath.Join(workRoot, fmt.Sprintf("node%d.stdout.log", index+1)),
			stderrPath:    filepath.Join(workRoot, fmt.Sprintf("node%d.stderr.log", index+1)),
		}
		if err := server.start(); err != nil {
			return nil, fmt.Errorf("start %s: %w", spec.id, err)
		}
		managed.servers = append(managed.servers, server)
	}
	for _, spec := range specs {
		if err := waitForPort(spec.address, 30*time.Second); err != nil {
			return nil, err
		}
	}
	// Allow the election timeout to elapse so a leader is available before
	// the first workload request is sent.
	time.Sleep(2 * time.Second)
	cleanup = false
	return managed, nil
}

func seedManagedResources(
	workRoot string,
	mode string,
	resource string,
	resourcePrefix string,
	resourceCount int,
) error {
	absoluteWorkRoot, err := filepath.Abs(workRoot)
	if err != nil {
		return fmt.Errorf("resolve resource work root: %w", err)
	}
	for index := 0; index < 3; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		seededResources := make([]storage.ResourceLocation, 0, resourceCount)
		if mode == "multi" || mode == "verify-multi" || mode == "restart-recovery" {
			for resourceIndex := 0; resourceIndex < resourceCount; resourceIndex++ {
				resourceID := fmt.Sprintf("%s-%04d", resourcePrefix, resourceIndex)
				location := filepath.Join(absoluteWorkRoot, fmt.Sprintf("node%d", index+1), "data", resourceID+".log")
				seededResources = append(seededResources, storage.ResourceLocation{ResourceID: resourceID, Location: location})
			}
		} else {
			location := filepath.Join(absoluteWorkRoot, fmt.Sprintf("node%d", index+1), "data", resource+".log")
			seededResources = append(seededResources, storage.ResourceLocation{ResourceID: resource, Location: location})
		}
		applicationPath := filepath.Join(absoluteWorkRoot, fmt.Sprintf("node%d", index+1), "data", "application")
		applicationStore, err := storage.OpenMemoryApplicationStore(applicationPath)
		if err != nil {
			cancel()
			return fmt.Errorf("open application store %d: %w", index+1, err)
		}
		if err := applicationStore.SaveResourceLocations(ctx, seededResources); err != nil {
			applicationStore.Close()
			cancel()
			return fmt.Errorf("seed embedded resources %d: %w", index+1, err)
		}
		if err := applicationStore.Close(); err != nil {
			cancel()
			return fmt.Errorf("close application store %d: %w", index+1, err)
		}
		cancel()
	}
	return nil
}

func main() {
	addresses := flag.String("addresses", "localhost:5001,localhost:5002,localhost:5003", "comma-separated server addresses")
	mode := flag.String("mode", "sanity", "sanity, stress, multi, verify, verify-multi, failover, follower-recovery, restart-recovery, or no-quorum")
	resource := flag.String("resource", "test-resource-20261002", "resource id")
	requests := flag.Int("requests", 1000, "stress request count")
	expectedCount := flag.Int("expected-count", 0, "expected application-log message count in verify mode")
	concurrency := flag.Int("concurrency", 32, "stress concurrency")
	clients := flag.Int("clients", 1000, "multi logical client count")
	messages := flag.Int("messages", 100, "multi messages per client")
	resources := flag.Int("resources", 1000, "multi resource count")
	lockBatch := flag.Int("lock-batch", 10, "multi messages per lock acquisition")
	timeout := flag.Duration("timeout", 2*time.Hour, "multi workload and validation timeout")
	snapshotThreshold := flag.Int("snapshot-threshold", 100000, "managed server Raft entries between snapshots; zero disables snapshots")
	proposalBatchWait := flag.Duration("proposal-batch-wait", time.Millisecond, "managed server maximum proposal batching delay")
	proposalBatchSize := flag.Int("proposal-batch-size", 256, "managed server adaptive ready batch size")
	proposalBatchMax := flag.Int("proposal-batch-max", 1024, "managed server maximum proposal batch size")
	resourcePrefix := flag.String("resource-prefix", "load-resource", "multi resource ID prefix")
	manageServers := flag.Bool("manage-servers", false, "start and stop an isolated three-node cluster")
	reuseData := flag.Bool("reuse-data", false, "reuse an existing managed cluster directory without renaming or seeding resources")
	serverExecutable := flag.String("server-exe", ".test-artifacts/server.exe", "server executable used with --manage-servers")
	serverWorkRoot := flag.String("server-work-root", `.test-artifacts\managed-cluster`, "working directory for managed servers")
	flag.Parse()

	var managed *managedServers
	var err error
	if *manageServers {
		if *addresses != "localhost:5001,localhost:5002,localhost:5003" {
			log.Fatal("--manage-servers currently requires the default addresses localhost:5001,localhost:5002,localhost:5003")
		}
		if !*reuseData {
			runID := time.Now().UnixNano()
			if *mode == "multi" || *mode == "restart-recovery" {
				*resourcePrefix = fmt.Sprintf("%s-%d", *resourcePrefix, runID)
			} else {
				*resource = fmt.Sprintf("%s-%d", *resource, runID)
			}
			if *serverWorkRoot == `.test-artifacts\managed-cluster` {
				*serverWorkRoot = filepath.Join(".test-artifacts", fmt.Sprintf("managed-cluster-%d", runID))
			}
			resourceCount := 1
			if *mode == "multi" || *mode == "restart-recovery" {
				resourceCount = *resources
			}
			if err := seedManagedResources(
				*serverWorkRoot, *mode, *resource, *resourcePrefix, resourceCount,
			); err != nil {
				log.Fatal(err)
			}
		}
		managed, err = startManagedServers(
			*serverExecutable, *serverWorkRoot, *snapshotThreshold,
			*proposalBatchWait, *proposalBatchSize, *proposalBatchMax,
		)
		if err != nil {
			log.Fatal(err)
		}
		defer managed.Stop()
		log.Printf("managed cluster started; work root=%s resource=%s prefix=%s", *serverWorkRoot, *resource, *resourcePrefix)
	}

	c, err := newCluster(*addresses)
	if err != nil {
		if managed != nil {
			managed.Stop()
		}
		log.Fatal(err)
	}
	defer c.Close()

	switch *mode {
	case "sanity":
		err = runSanity(c, *resource)
	case "stress":
		err = runStress(c, *resource, *requests, *concurrency)
	case "multi":
		err = runMulti(c, *resourcePrefix, *clients, *messages, *resources, *concurrency, *lockBatch, *timeout)
	case "verify":
		err = runVerify(c, *resource, *expectedCount)
	case "verify-multi":
		if *clients <= 0 || *messages <= 0 || *resources <= 0 || *lockBatch <= 0 {
			err = errors.New("clients, messages, resources, and lock-batch must be positive")
			break
		}
		correctness, verifyErr := validateWorkload(
			c,
			*resourcePrefix,
			expectedWorkload(*resourcePrefix, *clients, *messages, *resources, *lockBatch),
			*timeout,
		)
		if verifyErr != nil {
			err = verifyErr
		} else {
			log.Printf("restart correctness=%s", correctness)
		}
	case "failover":
		err = runFailover(c, managed, *resource)
	case "follower-recovery":
		err = runFollowerRecovery(c, managed, *resource)
	case "restart-recovery":
		if *clients <= 0 || *messages <= 0 || *resources <= 0 || *concurrency <= 0 || *lockBatch <= 0 {
			err = errors.New("clients, messages, resources, concurrency, and lock-batch must be positive")
			break
		}
		err = runRestartRecovery(
			c, managed, *resourcePrefix, *clients, *messages, *resources,
			*concurrency, *lockBatch, *timeout,
		)
	case "no-quorum":
		err = runNoQuorum(c, managed, *resource)
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		if managed != nil {
			managed.Stop()
		}
		log.Fatal(err)
	}
}

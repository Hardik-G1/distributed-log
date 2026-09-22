package raft

import (
	"context"
	"errors"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
)

func (node *Node) ReadIndex(ctx context.Context) (int64, error) {
	if ctx == nil {
		return 0, errors.New("context is nil")
	}
	node.stateMu.Lock()
	if node.state.Role != RoleLeader {
		leaderID := node.state.LeaderID
		node.stateMu.Unlock()
		return 0, &NotLeaderError{
			LeaderID: leaderID,
		}
	}

	readTerm := node.state.CurrentTerm
	readIndex := node.state.CommitIndex
	peerIDs := make([]string, 0, len(node.state.Peers))
	for peerID := range node.state.Peers {
		peerIDs = append(peerIDs, peerID)
	}

	transport := node.transport

	node.stateMu.Unlock()
	if len(peerIDs) == 0 {
		return readIndex, nil
	}
	if transport == nil {
		return 0, errors.New("peer transport is nil")
	}

	requiredVotes := majority(len(peerIDs) + 1)
	confirmedVotes := 1
	if confirmedVotes >= requiredVotes {
		return readIndex, nil
	}

	resultCh := make(chan error, len(peerIDs))

	for _, peerID := range peerIDs {
		go func(peerID string) {
			request, requestTerm, err := node.buildAppendEntries(ctx, peerID)
			if err != nil {
				resultCh <- err
				return
			}
			if request == nil {
				resultCh <- &NotLeaderError{}
				return
			}
			if requestTerm != readTerm {
				resultCh <- errors.New("leader term changed")
				return
			}
			response, err := transport.SendAppendEntries(
				ctx,
				peerID,
				request,
			)
			if err != nil {
				resultCh <- err
				return
			}
			if response == nil {
				resultCh <- errors.New("empty append entries response")
				return
			}
			if response.CurrentTerm > readTerm {
				node.stateMu.Lock()
				stepDownErr := node.stepDownForTermLocked(
					ctx,
					response.CurrentTerm,
				)
				node.stateMu.Unlock()
				if stepDownErr != nil {
					resultCh <- stepDownErr
					return
				}
				resultCh <- &NotLeaderError{}
				return
			}
			if response.CurrentTerm != readTerm {
				resultCh <- errors.New("peer responded from a different term")
				return
			}

			if response.AppendStatus != pb.AppendResponse_APPEND_RESPONSE_SUCCESS {
				resultCh <- errors.New("peer did not acknowledge heartbeat")
				return
			}
			lastSentIndex := request.PrevLogIndex + int64(len(request.Entries))
			if err := node.handleAppendEntriesResponse(
				ctx,
				peerID,
				readTerm,
				lastSentIndex,
				response,
			); err != nil {
				resultCh <- err
				return
			}
			resultCh <- nil

		}(peerID)
	}
	responsesReceived := 0
	for confirmedVotes < requiredVotes {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case err := <-resultCh:
			responsesReceived++
			if err == nil {
				confirmedVotes++
				continue
			}
			var notLeaderErr *NotLeaderError
			if errors.As(err, &notLeaderErr) {
				return 0, err
			}
			remainingResponse := len(peerIDs) - responsesReceived
			if confirmedVotes+remainingResponse < requiredVotes {
				return 0, errors.New("read index quorum not reached")
			}
		}
	}
	node.stateMu.Lock()
	defer node.stateMu.Unlock()

	if node.state.Role != RoleLeader || node.state.CurrentTerm != readTerm {
		return 0, &NotLeaderError{LeaderID: node.state.LeaderID}
	}
	return readIndex, nil
}

package raft

import (
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

const maxCachedRaftLogs = 64 * 1024

func (node *Node) cacheRaftLogsLocked(
	entries []storage.RaftLog,
	replaceFrom int64,
) {
	if replaceFrom > 0 && len(node.logCache) > 0 {
		if replaceFrom <= node.logCacheBase {
			node.logCache = node.logCache[:0]
			node.logCacheBase = 0
		} else {
			keep := replaceFrom - node.logCacheBase
			if keep < int64(len(node.logCache)) {
				node.logCache = node.logCache[:int(keep)]
			}
		}
	}
	if len(entries) == 0 {
		return
	}
	expectedIndex := node.logCacheBase + int64(len(node.logCache))
	if len(node.logCache) == 0 || entries[0].LogIndex != expectedIndex {
		node.logCache = node.logCache[:0]
		node.logCacheBase = entries[0].LogIndex
	}

	for _, entry := range entries {
		entry.OperationPayload = append([]byte(nil), entry.OperationPayload...)
		node.logCache = append(node.logCache, entry)
	}
	if len(node.logCache) > maxCachedRaftLogs {
		drop := len(node.logCache) - maxCachedRaftLogs
		copy(node.logCache, node.logCache[drop:])
		node.logCache = node.logCache[:maxCachedRaftLogs]
		node.logCacheBase += int64(drop)
	}
	last := entries[len(entries)-1]
	node.lastLogIndex = last.LogIndex
	node.lastLogTerm = last.Term
}

func (node *Node) cachedRaftLogLocked(index int64) (storage.RaftLog, bool) {
	offset := index - node.logCacheBase
	if node.logCacheBase == 0 || offset < 0 || offset >= int64(len(node.logCache)) {
		return storage.RaftLog{}, false
	}
	entry := node.logCache[int(offset)]
	entry.OperationPayload = append([]byte(nil), entry.OperationPayload...)
	return entry, true
}

func (node *Node) cachedRaftEntriesLocked(
	index int64,
	limit int,
) ([]storage.RaftLog, bool) {
	offset := index - node.logCacheBase
	if node.logCacheBase == 0 || offset < 0 || offset >= int64(len(node.logCache)) {
		return nil, false
	}
	start := int(offset)
	end := len(node.logCache)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	entries := make([]storage.RaftLog, end-start)
	copy(entries, node.logCache[start:end])
	for index := range entries {
		entries[index].OperationPayload = append(
			[]byte(nil),
			entries[index].OperationPayload...,
		)
	}
	return entries, true
}

func (node *Node) discardCachedRaftLogsThroughLocked(
	index int64,
) {
	if len(node.logCache) == 0 || index < node.logCacheBase {
		return
	}
	drop := index - node.logCacheBase + 1
	if drop >= int64(len(node.logCache)) {
		node.logCache = node.logCache[:0]
		node.logCacheBase = 0
		return
	}
	dropCount := int(drop)
	copy(node.logCache, node.logCache[dropCount:])
	node.logCache = node.logCache[:len(node.logCache)-dropCount]
	node.logCacheBase += drop
}

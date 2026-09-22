package raft

import "errors"

var ErrLogMismatch = errors.New("raft log mismatch")
var ErrSnapshotRequired = errors.New("snapshot required")

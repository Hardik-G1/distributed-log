package main

import (
	"fmt"
	"testing"
)

func TestWorkloadResourceIsDeterministicWithinLockBatch(t *testing.T) {
	const (
		resources = 1000
		lockBatch = 8
	)

	first := workloadResource("test", 42, 0, resources, lockBatch)
	for sequence := 1; sequence < lockBatch; sequence++ {
		if got := workloadResource("test", 42, sequence, resources, lockBatch); got != first {
			t.Fatalf("sequence %d mapped to %q, want %q", sequence, got, first)
		}
	}
}

func TestMillionLogicalClientsCoverAllResources(t *testing.T) {
	const (
		clients   = 1_000_000
		resources = 1000
	)

	counts := make([]int, resources)
	for client := 0; client < clients; client++ {
		resource := workloadResource("million", client, 0, resources, 1)
		var resourceIndex int
		if _, err := fmt.Sscanf(resource, "million-%04d", &resourceIndex); err != nil {
			t.Fatalf("parse %q: %v", resource, err)
		}
		counts[resourceIndex]++
	}

	minimum, maximum := counts[0], counts[0]
	for resourceIndex, count := range counts {
		if count == 0 {
			t.Fatalf("resource %d received no clients", resourceIndex)
		}
		if count < minimum {
			minimum = count
		}
		if count > maximum {
			maximum = count
		}
	}
	if minimum < 850 || maximum > 1150 {
		t.Fatalf("distribution is unexpectedly skewed: min=%d max=%d", minimum, maximum)
	}
}

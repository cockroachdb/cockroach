// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationtestutils

import (
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// StreamKV is a key-value with a timestamp, used by stream tests (e.g. producer
// ordered buffer, txnfeed) to generate random sequences and checkpoints.
// Convert to *kvpb.RangeFeedValue or txnfeed's kvEvent as needed.
type StreamKV struct {
	Key      roachpb.Key
	WallTime int64
	Value    []byte
}

// CheckpointInsertion describes a checkpoint wall time inserted after a KV at
// AfterIndex. For an empty KV list, AfterIndex is -1.
type CheckpointInsertion struct {
	AfterIndex int
	WallTime   int64
}

// StreamSeqOptions configures GenerateRandomKVSequence. Zero values use
// defaults.
type StreamSeqOptions struct {
	// MaxTxnSize is the maximum number of KVs sharing the same timestamp
	// (simulating a transaction). If zero, defaults to 3.
	MaxTxnSize int
	// KeyPrefix is prepended to every generated key. If nil, /a/ is used.
	KeyPrefix roachpb.Key
}

// RandKeyWithPrefix returns a random key within [prefix, prefix.PrefixEnd()):
// it appends a 16-byte random suffix (UUID-sized) so collisions are negligible.
// Mirrors txnfeed's randKeyWithPrefix.
func RandKeyWithPrefix(rng *rand.Rand, prefix roachpb.Key) roachpb.Key {
	suffix := make([]byte, 16)
	for i := range suffix {
		suffix[i] = byte(rng.Intn(256))
	}
	return append(append([]byte(nil), prefix...), suffix...)
}

// GenerateRandomKVSequence creates a random sequence of KVs in "transaction"
// groups: groups of 1..MaxTxnSize KVs share the same timestamp, then the next
// group gets the next timestamp. Keys are KeyPrefix + 16-byte random suffix.
// Returns the slice (order: by timestamp group, then key) and the max
// timestamp used. Callers may shuffle the slice to simulate out-of-order feed.
func GenerateRandomKVSequence(
	rng *rand.Rand, numKVs int, opts StreamSeqOptions,
) (kvs []StreamKV, maxWallTime int64) {
	if opts.MaxTxnSize <= 0 {
		opts.MaxTxnSize = 3
	}
	if len(opts.KeyPrefix) == 0 {
		opts.KeyPrefix = roachpb.Key("/a/")
	}
	var out []StreamKV
	wall := int64(1)
	for len(out) < numKVs {
		kvsLeft := numKVs - len(out)
		maxGroup := kvsLeft
		if opts.MaxTxnSize < maxGroup {
			maxGroup = opts.MaxTxnSize
		}
		groupSize := 1 + rng.Intn(maxGroup)
		for i := 0; i < groupSize; i++ {
			key := RandKeyWithPrefix(rng, opts.KeyPrefix)
			out = append(out, StreamKV{
				Key:      key,
				WallTime: wall,
				Value:    []byte("v"),
			})
		}
		wall++
	}
	maxWallTime = wall - 1
	return out, maxWallTime
}

// GenerateResolvedTimestamps returns numCheckpoints sorted wall times for the
// given KV sequence. It trims to the requested count while preserving overall
// timestamp coverage.
func GenerateResolvedTimestamps(rng *rand.Rand, kvs []StreamKV, numCheckpoints int) []int64 {
	if len(kvs) == 0 || numCheckpoints <= 0 {
		return nil
	}

	wallTimes := make([]int64, len(kvs))
	for i := range kvs {
		wallTimes[i] = kvs[i].WallTime
	}

	density := float64(numCheckpoints) / float64(len(kvs))
	if density > 1 {
		density = 1
	}
	insertions := GenerateCheckpointInsertions(rng, density, wallTimes)
	seen := make(map[int64]struct{}, len(insertions))
	unique := make([]int64, 0, len(insertions))
	for _, ins := range insertions {
		if _, ok := seen[ins.WallTime]; ok {
			continue
		}
		seen[ins.WallTime] = struct{}{}
		unique = append(unique, ins.WallTime)
	}
	slices.Sort(unique)
	n := numCheckpoints
	if n > len(unique) {
		n = len(unique)
	}
	if n == 0 {
		return nil
	}
	result := make([]int64, n)
	if n == 1 {
		result[0] = unique[len(unique)-1]
		return result
	}
	for i := 0; i < n; i++ {
		idx := (i * (len(unique) - 1)) / (n - 1)
		result[i] = unique[idx]
	}
	return result
}

// GenerateCheckpointInsertions emits checkpoints after KV indexes with probability
// density, while ensuring no checkpoint resolves future KVs.
func GenerateCheckpointInsertions(
	rng *rand.Rand, density float64, wallTimes []int64,
) []CheckpointInsertion {
	if len(wallTimes) == 0 {
		return []CheckpointInsertion{{AfterIndex: -1, WallTime: 1}}
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(1))
	}

	maxTS := wallTimes[0]
	for _, t := range wallTimes[1:] {
		if t > maxTS {
			maxTS = t
		}
	}

	maxCheckpointAt := make([]int64, len(wallTimes))
	minSoFar := maxTS
	for i := len(wallTimes) - 1; i >= 0; i-- {
		maxCheckpointAt[i] = minSoFar
		if wallTimes[i] < minSoFar {
			minSoFar = wallTimes[i]
		}
	}

	lastCheckpoint := int64(0)
	result := make([]CheckpointInsertion, 0, len(wallTimes)+1)
	for i := range wallTimes {
		if maxCheckpointAt[i] == lastCheckpoint {
			continue
		}
		if density < rng.Float64() {
			continue
		}
		delta := int(maxCheckpointAt[i] - lastCheckpoint)
		lastCheckpoint = int64(rng.Intn(delta)) + lastCheckpoint
		result = append(result, CheckpointInsertion{AfterIndex: i, WallTime: lastCheckpoint})
	}
	result = append(result, CheckpointInsertion{
		AfterIndex: len(wallTimes) - 1,
		WallTime:   maxTS,
	})
	return result
}

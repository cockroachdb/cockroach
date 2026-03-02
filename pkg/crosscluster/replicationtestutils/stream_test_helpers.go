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
// Mirrors txnfeed's generateMergeFeedInputs KV generation.
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

// GenerateResolvedTimestamps returns numCheckpoints sorted wall times that are
// valid as resolved/checkpoint timestamps for the given KV sequence (each is
// <= some KV's timestamp). It collects unique timestamps from kvs, sorts them,
// then picks up to numCheckpoints evenly spread (or all if fewer). Useful for
// producer ordered buffer tests and txnfeed-style tests.
func GenerateResolvedTimestamps(rng *rand.Rand, kvs []StreamKV, numCheckpoints int) []int64 {
	if len(kvs) == 0 || numCheckpoints <= 0 {
		return nil
	}
	seen := make(map[int64]struct{})
	for _, kv := range kvs {
		seen[kv.WallTime] = struct{}{}
	}
	unique := make([]int64, 0, len(seen))
	for t := range seen {
		unique = append(unique, t)
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
	for i := 0; i < n; i++ {
		idx := (i * (len(unique) - 1)) / (n - 1)
		if n == 1 {
			idx = 0
		}
		result[i] = unique[idx]
	}
	return result
}

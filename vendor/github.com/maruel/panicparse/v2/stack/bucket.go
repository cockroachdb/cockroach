// Copyright 2015 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package stack

import (
	"sort"
)

// Similarity is the level at which two call lines arguments must match to be
// considered similar enough to coalesce them.
type Similarity int

const (
	// ExactFlags requires same bits (e.g. Locked).
	ExactFlags Similarity = iota
	// ExactLines requests the exact same arguments on the call line.
	ExactLines
	// AnyPointer considers different pointers a similar call line.
	AnyPointer
	// AnyValue accepts any value as similar call line.
	AnyValue
)

// Aggregated is a list of Bucket sorted by repetition count.
type Aggregated struct {
	// Snapshot is a pointer to the structure that was used to generate these
	// buckets.
	*Snapshot

	Buckets []*Bucket

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// Aggregate merges similar goroutines into buckets.
//
// The buckets are ordered in library provided order of relevancy. You can
// reorder at your choosing.
func (s *Snapshot) Aggregate(similar Similarity) *Aggregated {
	type count struct {
		ids   []int
		first bool
	}
	b := map[*Signature]*count{}
	// O(n²). Fix eventually.
	for _, routine := range s.Goroutines {
		found := false
		for key, c := range b {
			// When a match is found, this effectively drops the other goroutine ID.
			if key.similar(&routine.Signature, similar) {
				found = true
				c.ids = append(c.ids, routine.ID)
				c.first = c.first || routine.First
				if !key.equal(&routine.Signature) {
					// Almost but not quite equal. There's different pointers passed
					// around but the same values. Zap out the different values.
					newKey := key.merge(&routine.Signature)
					b[newKey] = c
					delete(b, key)
				}
				break
			}
		}
		if !found {
			// Create a copy of the Signature, since it will be mutated.
			key := &Signature{}
			*key = routine.Signature
			b[key] = &count{ids: []int{routine.ID}, first: routine.First}
		}
	}
	bs := make([]*Bucket, 0, len(b))
	for signature, c := range b {
		sort.Ints(c.ids)
		bs = append(bs, &Bucket{Signature: *signature, IDs: c.ids, First: c.first})
	}
	// Do reverse sort.
	sort.SliceStable(bs, func(i, j int) bool {
		l := bs[i]
		r := bs[j]
		if l.First || r.First {
			return l.First
		}
		if l.Signature.less(&r.Signature) {
			return true
		}
		if r.Signature.less(&l.Signature) {
			return false
		}
		return len(r.IDs) > len(l.IDs)
	})
	return &Aggregated{
		Snapshot: s,
		Buckets:  bs,
	}
}

// Bucket is a stack trace signature and the list of goroutines that fits this
// signature.
type Bucket struct {
	// Signature is the generalized signature for this bucket.
	Signature
	// IDs is the ID of each Goroutine with this Signature.
	IDs []int
	// First is true if this Bucket contains the first goroutine, e.g. the one
	// Signature that likely generated the panic() call, if any.
	First bool

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

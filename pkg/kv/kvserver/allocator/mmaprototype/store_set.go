// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Ordered and de-duped list of storeIDs. Represents a set of stores. Used for
// fast set operations for constraint satisfaction.
type storeSet []roachpb.StoreID

func makeStoreSet(a []roachpb.StoreID) storeSet {
	slices.Sort(a)
	return a
}

func (s *storeSet) union(b storeSet) {
	a := *s
	n := len(a)
	m := len(b)
	for i, j := 0, 0; j < m; {
		if i < n && a[i] < b[j] {
			i++
			continue
		}
		// i >= n || a[i] >= b[j]
		if i >= n || a[i] > b[j] {
			a = append(a, b[j])
			j++
			continue
		}
		// a[i] == b[j]
		i++
		j++
	}
	if len(a) > n {
		slices.Sort(a)
		*s = a
	}
}

func (s *storeSet) intersect(b storeSet) {
	// TODO(sumeer): For larger lists, probe using smaller list.
	a := *s
	n := len(a)
	m := len(b)
	k := 0
	for i, j := 0, 0; i < n && j < m; {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			a[k] = a[i]
			i++
			j++
			k++
		}
	}
	*s = a[:k]
}

func (s *storeSet) isEqual(b storeSet) bool {
	a := *s
	n := len(a)
	m := len(b)
	if n != m {
		return false
	}
	for i := range b {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Returns true iff found (and successfully removed).
func (s *storeSet) remove(storeID roachpb.StoreID) bool {
	a := *s
	n := len(a)
	found := false
	for i := range a {
		if a[i] == storeID {
			// INVARIANT: i < n, so i <= n-1 and i+1 <= n.
			copy(a[i:n-1], a[i+1:n])
			found = true
			break
		}
	}
	if !found {
		return false
	}
	*s = a[:n-1]
	return true
}

// Returns true iff the storeID was not already in the set.
func (s *storeSet) insert(storeID roachpb.StoreID) bool {
	a := *s
	n := len(a)
	var pos int
	for pos = 0; pos < n; pos++ {
		if storeID < a[pos] {
			break
		} else if storeID == a[pos] {
			return false
		}
	}
	var b storeSet
	if cap(a) > n {
		b = a[:n+1]
	} else {
		m := 2 * cap(a)
		const minLength = 10
		if m < minLength {
			m = minLength
		}
		b = make([]roachpb.StoreID, n+1, m)
		// Insert at pos, so pos-1 is the last element before the insertion.
		if pos > 0 {
			copy(b[:pos], a[:pos])
		}
	}
	copy(b[pos+1:n+1], a[pos:n])
	b[pos] = storeID
	*s = b
	return true
}

func (s *storeSet) contains(storeID roachpb.StoreID) bool {
	_, found := slices.BinarySearch(*s, storeID)
	return found
}

const ( // offset64 is the initial hash value, and is taken from fnv.go
	offset64 = 14695981039346656037

	// prime64 is a large-ish prime number used in hashing and taken from fnv.go.
	prime64 = 1099511628211
)

// FNV-1a hash algorithm.
func (s *storeSet) hash() uint64 {
	h := uint64(offset64)
	for _, storeID := range *s {
		h ^= uint64(storeID)
		h *= prime64
	}
	return h
}

// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLockIsStale(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	for _, tc := range []struct {
		name       string
		acquiredAt time.Time
		want       bool
	}{
		{"fresh", now.Add(-time.Minute), false},
		{"just under ttl", now.Add(-lockTTL + time.Second), false},
		{"exactly ttl", now.Add(-lockTTL), false},
		{"just over ttl", now.Add(-lockTTL - time.Second), true},
		{"ancient", now.Add(-24 * time.Hour), true},
		{"future clock skew", now.Add(time.Minute), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := lockIsStale(tc.acquiredAt, now, lockTTL); got != tc.want {
				t.Errorf("lockIsStale(%s, %s, %s) = %v, want %v",
					tc.acquiredAt, now, lockTTL, got, tc.want)
			}
		})
	}
}

func TestLockMetadataRoundTrip(t *testing.T) {
	in := lockMetadata{
		Version:    "v24.1.31",
		RunID:      "1234567890",
		AcquiredAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out lockMetadata
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Version != in.Version || out.RunID != in.RunID || !out.AcquiredAt.Equal(in.AcquiredAt) {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
}

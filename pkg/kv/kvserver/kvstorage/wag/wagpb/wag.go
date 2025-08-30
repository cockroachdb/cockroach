// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wagpb

import (
	// TODO(pav-kv): remove this once the dev gen bazel picks up this dependency
	// from the .pb.go file.
	_ "github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/redact"
)

// String implements the fmt.Stringer interface.
func (a Addr) String() string {
	return redact.StringWithoutMarkers(a)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (a Addr) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%d/%d:%d", a.RangeID, a.ReplicaID, a.Index)
}

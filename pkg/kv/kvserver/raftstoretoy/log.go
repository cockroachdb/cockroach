// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/rspb"

// ReplicaPos reflects the durable state of a Replica as persisted in the log
// engine. The aim of WAG recovery is to replay operations from the WAG to
// this end state.
type ReplicaPos struct {
	ID       rspb.FullLogID
	WAGIndex rspb.WAGIndex // WAG operation to replay to
}

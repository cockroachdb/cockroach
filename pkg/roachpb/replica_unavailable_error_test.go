// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func TestReplicaUnavailableError(t *testing.T) {
	ctx := context.Background()
	skip.UnderBazelWithIssue(t, 75108, "flaky test")
	var _ = (*ReplicaUnavailableError)(nil)
	rDesc := ReplicaDescriptor{NodeID: 1, StoreID: 2, ReplicaID: 3}
	var set ReplicaSet
	set.AddReplica(rDesc)
	desc := NewRangeDescriptor(123, RKeyMin, RKeyMax, set)

	var err error = NewReplicaUnavailableError(desc, rDesc)
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, err))

	s := fmt.Sprintf("%s\n%s", err, redact.Sprint(err))
	echotest.Require(t, s, filepath.Join("testdata", "replica_unavailable_error.txt"))
}

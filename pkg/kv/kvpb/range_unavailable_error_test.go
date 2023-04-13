// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvpb

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func TestRangeUnavailableError(t *testing.T) {
	ctx := context.Background()
	var _ = (*RangeUnavailableError)(nil)
	rDesc := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 2, ReplicaID: 3}
	var set roachpb.ReplicaSet
	set.AddReplica(rDesc)
	desc := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax, set)

	errRangeUnavailable := errors.New("too many failed range sending attempts with no response")
	var err = NewRangeUnavailableError(errRangeUnavailable, desc)
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, err))

	s := fmt.Sprintf("%s\n%s", err, redact.Sprint(err))
	echotest.Require(t, s, filepath.Join("testdata", "range_unavailable_error.txt"))
}

// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestCalculateThreshold(t *testing.T) {
	for _, c := range []struct {
		ttlSeconds int32
		ts         hlc.Timestamp
	}{
		{
			ts:         hlc.Timestamp{WallTime: time.Hour.Nanoseconds(), Logical: 0},
			ttlSeconds: 1,
		},
	} {
		policy := zonepb.GCPolicy{TTLSeconds: c.ttlSeconds}
		require.Equal(t, c.ts, TimestampForThreshold(CalculateThreshold(c.ts, policy), policy))
	}
}

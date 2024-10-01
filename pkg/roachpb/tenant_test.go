// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTenantIDString(t *testing.T) {
	for tID, expStr := range map[TenantID]string{
		{}:                               "invalid",
		SystemTenantID:                   "system",
		MustMakeTenantID(2):              "2",
		MustMakeTenantID(999):            "999",
		MustMakeTenantID(math.MaxUint64): "18446744073709551615",
	} {
		require.Equal(t, tID.InternalValue != 0, tID.IsSet())
		require.Equal(t, expStr, tID.String())
	}
}

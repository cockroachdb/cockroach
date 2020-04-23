// Copyright 2020 The Cockroach Authors.
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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTenantIDString(t *testing.T) {
	for tID, expStr := range map[TenantID]string{
		SystemTenantID: "system",
		2:              "2",
		999:            "999",
		math.MaxUint64: "18446744073709551615",
	} {
		require.Equal(t, expStr, tID.String())
	}
}

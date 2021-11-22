// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDescriptorIDSetString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		ids DescriptorIDSet
		exp string
	}{
		{
			ids: MakeDescriptorIDSet(),
			exp: "{}",
		},
		{
			ids: MakeDescriptorIDSet(1),
			exp: "{1}",
		},
		{
			ids: MakeDescriptorIDSet(10000, 1, 100, 1000, 10, 100000),
			exp: "{1, 10, 100, 1000, 10000, 100000}",
		},
	} {
		require.Equal(t, tc.exp, tc.ids.String())
	}
}

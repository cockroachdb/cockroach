// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestShouldSplitAtDesc(t *testing.T) {
	for inner, should := range map[DescriptorProto]bool{
		&TableDescriptor{}:                    true,
		&TableDescriptor{ViewQuery: "SELECT"}: false,
		&DatabaseDescriptor{}:                 false,
		&TypeDescriptor{}:                     false,
		&SchemaDescriptor{}:                   false,
	} {
		var rawDesc roachpb.Value
		require.NoError(t, rawDesc.SetProto(WrapDescriptor(inner)))
		require.Equal(t, should, ShouldSplitAtDesc(&rawDesc))
	}
}

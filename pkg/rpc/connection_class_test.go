// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConnectionClassFromProto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	want := map[kvpb.ConnectionClass]ConnectionClass{
		kvpb.ConnectionClass_DEFAULT:   DefaultClass,
		kvpb.ConnectionClass_BULK_DATA: BulkDataClass,
	}
	// The *_name map generated from a protobuf is keyed by the value of the enum.
	// Iterate this map to get all the values of the enum, so that new values are
	// never forgotten.
	for val := range kvpb.ConnectionClass_name {
		cc := kvpb.ConnectionClass(val)
		want, found := want[cc]
		require.True(t, found)
		require.Equal(t, want, ConnectionClassFromProto(cc))
	}
	// Unknown values should translate to the default connection class.
	unknown := kvpb.ConnectionClass(-1)
	require.NotContains(t, want, unknown)
	require.Equal(t, DefaultClass, ConnectionClassFromProto(unknown))
}

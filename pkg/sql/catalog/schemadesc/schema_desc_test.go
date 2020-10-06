// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemadesc_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSafeMessage(t *testing.T) {
	for _, tc := range []struct {
		desc catalog.SchemaDescriptor
		exp  string
	}{
		{
			desc: schemadesc.NewImmutable(descpb.SchemaDescriptor{
				ID:            12,
				Version:       1,
				ParentID:      2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "foo",
			}),
			exp: "schemadesc.Immutable: {ID: 12, Version: 1, ModificationTime: \"0,0\", ParentID: 2, State: OFFLINE, OfflineReason: \"foo\"}",
		},
		{
			desc: schemadesc.NewCreatedMutable(descpb.SchemaDescriptor{
				ID:            42,
				Version:       1,
				ParentID:      2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "bar",
			}),
			exp: "schemadesc.Mutable: {ID: 42, Version: 1, IsUncommitted: true, ModificationTime: \"0,0\", ParentID: 2, State: OFFLINE, OfflineReason: \"bar\"}",
		},
	} {
		t.Run("", func(t *testing.T) {
			redacted := string(redact.Sprint(tc.desc).Redact())
			require.Equal(t, tc.exp, redacted)
			{
				var m map[string]interface{}
				require.NoError(t, yaml.UnmarshalStrict([]byte(redacted), &m))
			}
		})
	}
}

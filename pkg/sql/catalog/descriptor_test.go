// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestFormatSafeDescriptorProperties(t *testing.T) {
	for _, tc := range []struct {
		desc catalog.Descriptor
		exp  string
	}{
		{
			desc: tabledesc.NewBuilder(&descpb.TableDescriptor{
				ID:       27,
				Version:  2,
				ParentID: 12,
				State:    descpb.DescriptorState_ADD,
			}).BuildImmutable(),
			exp: "ID: 27, Version: 2, ModificationTime: \"0,0\", ParentID: 12, ParentSchemaID: 29, State: ADD",
		},
		{
			desc: schemadesc.NewBuilder(&descpb.SchemaDescriptor{
				ID:            12,
				Version:       1,
				ParentID:      2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "foo",
			}).BuildImmutable(),
			exp: "ID: 12, Version: 1, ModificationTime: \"0,0\", ParentID: 2, State: OFFLINE, OfflineReason: \"foo\"",
		},
		{
			desc: dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
				ID:      12,
				Version: 1,
				State:   descpb.DescriptorState_PUBLIC,
			}).BuildCreatedMutable(),
			exp: "ID: 12, Version: 1, IsUncommitted: true, ModificationTime: \"0,0\", State: PUBLIC",
		},
		{
			desc: func() catalog.Descriptor {
				desc := tabledesc.NewBuilder(&descpb.TableDescriptor{
					ID:                      27,
					Version:                 2,
					ParentID:                12,
					UnexposedParentSchemaID: 51,
					State:                   descpb.DescriptorState_PUBLIC,
				}).BuildExistingMutableTable()
				desc.MaybeIncrementVersion()
				desc.AddDrainingName(descpb.NameInfo{
					ParentID:       12,
					ParentSchemaID: 51,
				})
				return desc.ImmutableCopy()
			}(),
			exp: "ID: 27, Version: 3, IsUncommitted: true, ModificationTime: \"0,0\", ParentID: 12, ParentSchemaID: 51, State: PUBLIC, NumDrainingNames: 1",
		},
	} {
		t.Run("", func(t *testing.T) {
			var buf redact.StringBuilder
			catalog.FormatSafeDescriptorProperties(&buf, tc.desc)
			redacted := string(buf.RedactableString().Redact())
			require.Equal(t, tc.exp, redacted)
			var m map[string]interface{}
			require.NoError(t, yaml.UnmarshalStrict([]byte("{"+redacted+"}"), &m))
		})
	}
}

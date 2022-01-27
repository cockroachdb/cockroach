// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package typedesc_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSafeMessage(t *testing.T) {
	for _, tc := range []struct {
		desc catalog.TypeDescriptor
		exp  string
	}{
		{
			desc: typedesc.NewBuilder(&descpb.TypeDescriptor{
				Name:                     "foo",
				ID:                       21,
				Version:                  3,
				Privileges:               catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
				ParentID:                 2,
				ParentSchemaID:           29,
				ArrayTypeID:              117,
				State:                    descpb.DescriptorState_PUBLIC,
				Kind:                     descpb.TypeDescriptor_ALIAS,
				ReferencingDescriptorIDs: []descpb.ID{73, 37},
			}).BuildImmutableType(),
			exp: `typedesc.immutable: {ID: 21, Version: 3, ModificationTime: "0,0", ` +
				`ParentID: 2, ParentSchemaID: 29, State: PUBLIC, ` +
				`Kind: ALIAS, ArrayTypeID: 117, ReferencingDescriptorIDs: [73, 37]}`,
		},
		{
			desc: typedesc.NewBuilder(&descpb.TypeDescriptor{
				Name:                     "foo",
				ID:                       21,
				Version:                  3,
				Privileges:               catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
				ParentID:                 2,
				ParentSchemaID:           29,
				ArrayTypeID:              117,
				State:                    descpb.DescriptorState_PUBLIC,
				Kind:                     descpb.TypeDescriptor_ENUM,
				ReferencingDescriptorIDs: []descpb.ID{73, 37},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{},
				},
			}).BuildImmutableType(),
			exp: `typedesc.immutable: {ID: 21, Version: 3, ModificationTime: "0,0", ` +
				`ParentID: 2, ParentSchemaID: 29, State: PUBLIC, ` +
				`Kind: ENUM, NumEnumMembers: 1, ArrayTypeID: 117, ReferencingDescriptorIDs: [73, 37]}`,
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

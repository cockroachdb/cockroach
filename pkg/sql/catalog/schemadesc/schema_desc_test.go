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
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSafeMessage(t *testing.T) {
	for _, tc := range []struct {
		desc catalog.Descriptor
		exp  string
	}{
		{
			desc: schemadesc.NewBuilder(&descpb.SchemaDescriptor{
				ID:            12,
				Version:       1,
				ParentID:      2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "foo",
			}).BuildImmutable(),
			exp: "schemadesc.Immutable: {ID: 12, Version: 1, ModificationTime: \"0,0\", ParentID: 2, State: OFFLINE, OfflineReason: \"foo\"}",
		},
		{
			desc: schemadesc.NewBuilder(&descpb.SchemaDescriptor{
				ID:            42,
				Version:       1,
				ParentID:      2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "bar",
			}).BuildCreatedMutable(),
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

func TestValidateCrossSchemaReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tests := []struct {
		err    string
		desc   descpb.SchemaDescriptor
		dbDesc descpb.DatabaseDescriptor
	}{
		{ // 1
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52},
				},
			},
		},
		{ // 2
			err: `parent database (500): referenced database ID 500: descriptor not found`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 500,
				Name:     "schema1",
			},
		},
		{ // 3
			err: `parent database (51): matching schema entry not found`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
			},
		},
		{ // 4
			err: `parent database (51): matching schema entry not found`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"foo": {ID: 52, Dropped: true},
				},
			},
		},
		{ // 5
			err: `parent database (51): schema mapping entry "schema1" (52): marked as dropped`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52, Dropped: true},
				},
			},
		},
		{ // 6
			err: `parent database (51): schema mapping entry "bad" (52): name mismatch`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"bad": {ID: 52},
				},
			},
		},
		{ // 7
			err: `parent database (51): schema mapping entry "schema1" (500): ID mismatch`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 500},
				},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("#%d %s", i+1, test.err), func(t *testing.T) {
			privilege := descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName())
			descs := catalog.MakeMapDescGetter()
			test.desc.Privileges = privilege
			desc := schemadesc.NewBuilder(&test.desc).BuildImmutable()
			descs.Descriptors[test.desc.ID] = desc
			test.dbDesc.Privileges = privilege
			descs.Descriptors[test.dbDesc.ID] = dbdesc.NewBuilder(&test.dbDesc).BuildImmutable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
			const validateCrossReferencesOnly = catalog.ValidationLevelCrossReferences &^ (catalog.ValidationLevelCrossReferences >> 1)
			if err := catalog.Validate(ctx, descs, validateCrossReferencesOnly, desc).CombinedError(); err == nil {
				if test.err != "" {
					t.Errorf("expected \"%s\", but found success: %+v", expectedErr, test.desc)
				}
			} else if expectedErr != err.Error() {
				t.Errorf("expected \"%s\", but found \"%s\"", expectedErr, err.Error())
			}
		})
	}
}

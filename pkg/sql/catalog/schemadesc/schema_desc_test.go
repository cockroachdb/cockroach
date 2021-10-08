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
			exp: "schemadesc.immutable: {ID: 12, Version: 1, ModificationTime: \"0,0\", ParentID: 2, State: OFFLINE, OfflineReason: \"foo\"}",
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
		{ // 0
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
		{ // 1
			err: `referenced database ID 500: descriptor not found`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 500,
				Name:     "schema1",
			},
		},
		{ // 2
			err: `not present in parent database [51] schemas mapping`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
			},
		},
		{ // 2
			err: `not present in parent database [51] schemas mapping`,
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
		{ // 3
			err: `present in parent database [51] schemas mapping but marked as dropped`,
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
		{ // 4
			err: `present in parent database [51] schemas mapping but under name "bad"`,
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
		{ // 5
			err: `present in parent database [51] schemas mapping but name maps to other schema [500]`,
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
		privilege := descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName())
		descs := catalog.MakeMapDescGetter()
		test.desc.Privileges = privilege
		desc := schemadesc.NewBuilder(&test.desc).BuildImmutable()
		descs.Descriptors[test.desc.ID] = desc
		test.dbDesc.Privileges = privilege
		descs.Descriptors[test.dbDesc.ID] = dbdesc.NewBuilder(&test.dbDesc).BuildImmutable()
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		const validateCrossReferencesOnly = catalog.ValidationLevelCrossReferences &^ (catalog.ValidationLevelCrossReferences >> 1)
		results := catalog.Validate(ctx, descs, catalog.NoValidationTelemetry, validateCrossReferencesOnly, desc)
		if err := results.CombinedError(); err == nil {
			if test.err != "" {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
			}
		} else if expectedErr != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
		}
	}
}

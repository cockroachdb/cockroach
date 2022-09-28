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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
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

func TestValidateSchemaSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	defaultPrivilege := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
	invalidPrivilege := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
	invalidPrivilege.Grant(username.TestUserName(), privilege.List{privilege.SELECT}, false)

	tests := []struct {
		err  string
		desc descpb.SchemaDescriptor
	}{
		{ // 0
			err:  `empty schema name`,
			desc: descpb.SchemaDescriptor{},
		},
		{ // 1
			err: `invalid schema ID 0`,
			desc: descpb.SchemaDescriptor{
				Name: "schema1",
			},
		},
		{ // 2
			err: `privileges not set`,
			desc: descpb.SchemaDescriptor{
				ID:         52,
				Name:       "schema1",
				Privileges: nil,
			},
		},
		{ // 3
			err: `user testuser must not have SELECT privileges on schema "schema1"`,
			desc: descpb.SchemaDescriptor{
				ID:         52,
				ParentID:   51,
				Name:       "schema1",
				Privileges: invalidPrivilege,
			},
		},
		{ // 4
			err: `invalid function ID 0`,
			desc: descpb.SchemaDescriptor{
				ID:         52,
				ParentID:   51,
				Name:       "schema1",
				Privileges: defaultPrivilege,
				Functions: map[string]descpb.SchemaDescriptor_Function{
					"f": {Overloads: []descpb.SchemaDescriptor_FunctionOverload{{ID: 0}}},
				},
			},
		},
	}

	for i, test := range tests {
		var cb nstree.MutableCatalog
		desc := schemadesc.NewBuilder(&test.desc).BuildImmutable()
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		results := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, catalog.ValidationLevelSelfOnly, desc)
		if err := results.CombinedError(); err == nil {
			if test.err != "" {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
			}
		} else if expectedErr != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
		}
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
			err: `referenced database ID 500: referenced descriptor not found`,
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
		{ // 3
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
		{ // 4
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
		{ // 5
			err: `invalid function 500 in schema "schema1" (52)`,
			desc: descpb.SchemaDescriptor{
				ID:       52,
				ParentID: 51,
				Name:     "schema1",
				Functions: map[string]descpb.SchemaDescriptor_Function{
					"f": {Overloads: []descpb.SchemaDescriptor_FunctionOverload{{ID: 500}}},
				},
			},
			dbDesc: descpb.DatabaseDescriptor{
				ID: 51,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52},
				},
			},
		},
	}

	for i, test := range tests {
		privilege := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
		var cb nstree.MutableCatalog
		test.desc.Privileges = privilege
		desc := schemadesc.NewBuilder(&test.desc).BuildImmutable()
		cb.UpsertDescriptorEntry(desc)
		test.dbDesc.Privileges = privilege
		cb.UpsertDescriptorEntry(dbdesc.NewBuilder(&test.dbDesc).BuildImmutable())
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		const validateCrossReferencesOnly = catalog.ValidationLevelBackReferences &^ catalog.ValidationLevelSelfOnly
		results := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, validateCrossReferencesOnly, desc)
		if err := results.CombinedError(); err == nil {
			if test.err != "" {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
			}
		} else if expectedErr != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
		}
	}
}

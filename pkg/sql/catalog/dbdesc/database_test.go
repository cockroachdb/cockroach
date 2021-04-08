// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package dbdesc

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSafeMessage(t *testing.T) {
	for _, tc := range []struct {
		desc catalog.DatabaseDescriptor
		exp  string
	}{
		{
			desc: NewBuilder(&descpb.DatabaseDescriptor{
				ID:            12,
				Version:       1,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "foo",
			}).BuildImmutableDatabase(),
			exp: "dbdesc.immutable: {ID: 12, Version: 1, ModificationTime: \"0,0\", State: OFFLINE, OfflineReason: \"foo\"}",
		},
		{
			desc: NewBuilder(&descpb.DatabaseDescriptor{
				ID:            42,
				Version:       2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "bar",
			}).BuildCreatedMutableDatabase(),
			exp: "dbdesc.Mutable: {ID: 42, Version: 2, IsUncommitted: true, ModificationTime: \"0,0\", State: OFFLINE, OfflineReason: \"bar\"}",
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

func TestMakeDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOne("CREATE DATABASE test")
	if err != nil {
		t.Fatal(err)
	}
	const id = 17
	desc := NewInitial(id, string(stmt.AST.(*tree.CreateDatabase).Name), security.AdminRoleName())
	if desc.GetName() != "test" {
		t.Fatalf("expected Name == test, got %s", desc.GetName())
	}
	// ID is not set yet.
	if desc.GetID() != id {
		t.Fatalf("expected ID == %d, got %d", id, desc.GetID())
	}
	if len(desc.GetPrivileges().Users) != 2 {
		t.Fatalf("wrong number of privilege users, expected 2, got: %d", len(desc.GetPrivileges().Users))
	}
}

func TestValidateDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		err  string
		desc descpb.DatabaseDescriptor
	}{
		{`invalid database ID 0`,
			descpb.DatabaseDescriptor{
				Name:       "db",
				ID:         0,
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			},
		},
		{
			`primary region unset on a multi-region db 200`,
			descpb.DatabaseDescriptor{
				Name:         "multi-region-db",
				ID:           200,
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{},
				Privileges:   descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			},
		},
	}
	for i, d := range testData {
		t.Run(d.err, func(t *testing.T) {
			desc := NewBuilder(&d.desc).BuildImmutable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), d.err)
			if err := catalog.ValidateSelf(desc); err == nil {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, d.desc)
			} else if expectedErr != err.Error() {
				t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, expectedErr, err)
			}
		})
	}
}

func TestValidateCrossDatabaseReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tests := []struct {
		err             string
		desc            descpb.DatabaseDescriptor
		multiRegionEnum descpb.TypeDescriptor
		schemaDescs     []descpb.SchemaDescriptor
	}{
		{ // 0
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
			},
		},
		{ // 1
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52, Dropped: false},
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "schema1",
					ID:       52,
					ParentID: 51,
				},
			},
		},
		{ // 2
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 53, Dropped: true},
				},
			},
		},
		{ // 3
			err: `schema mapping entry "schema1" (500): referenced schema ID 500: descriptor not found`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 500, Dropped: false},
				},
			},
		},
		{ // 4
			err: `schema mapping entry "schema1" (52): schema name is actually "foo"`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52, Dropped: false},
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "foo",
					ID:       52,
					ParentID: 51,
				},
			},
		},
		{ // 5
			err: `schema mapping entry "schema1" (52): schema parentID is actually 500`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52},
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "schema1",
					ID:       52,
					ParentID: 500,
				},
			},
		},
		{ // 6
			err: `multi-region enum: referenced type ID 500: descriptor not found`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					RegionEnumID:  500,
					PrimaryRegion: "us-east-1",
				},
			},
		},
		{ // 7
			err: `multi-region enum: parentID is actually 500`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					RegionEnumID:  52,
					PrimaryRegion: "us-east-1",
				},
			},
			multiRegionEnum: descpb.TypeDescriptor{
				ID:       52,
				ParentID: 500,
			},
		},
		{ // 8
			err: `schema mapping entry "schema1" (53): referenced schema ID 53: descriptor is a *typedesc.immutable: unexpected descriptor type`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 53},
				},
			},
			multiRegionEnum: descpb.TypeDescriptor{
				ID:       53,
				ParentID: 51,
			},
		},
		{ // 9
			err: `multi-region enum: referenced type ID 53: descriptor is a *schemadesc.immutable: unexpected descriptor type`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					RegionEnumID:  53,
					PrimaryRegion: "us-east-1",
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "schema1",
					ID:       53,
					ParentID: 51,
				},
			},
		},
	}

	for i, test := range tests {
		privilege := descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName())
		descs := catalog.MakeMapDescGetter()
		test.desc.Privileges = privilege
		desc := NewBuilder(&test.desc).BuildImmutable()
		descs.Descriptors[test.desc.ID] = desc
		test.multiRegionEnum.Privileges = privilege
		descs.Descriptors[test.multiRegionEnum.ID] = typedesc.NewBuilder(&test.multiRegionEnum).BuildImmutable()
		for _, schemaDesc := range test.schemaDescs {
			schemaDesc.Privileges = privilege
			descs.Descriptors[schemaDesc.ID] = schemadesc.NewBuilder(&schemaDesc).BuildImmutable()
		}
		for _, desc := range descs.Descriptors {
			namespaceKey := descpb.NameInfo{
				ParentID:       desc.GetParentID(),
				ParentSchemaID: desc.GetParentSchemaID(),
				Name:           desc.GetName(),
			}
			descs.Namespace[namespaceKey] = desc.GetID()
		}
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		results := catalog.Validate(ctx, descs, catalog.NoValidationTelemetry, catalog.ValidationLevelAllPreTxnCommit, desc)
		if err := results.CombinedError(); err == nil {
			if test.err != "" {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
			}
		} else if expectedErr != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
		}
	}
}

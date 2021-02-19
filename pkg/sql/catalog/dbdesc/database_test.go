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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
			desc: NewImmutable(descpb.DatabaseDescriptor{
				ID:            12,
				Version:       1,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "foo",
			}),
			exp: "dbdesc.Immutable: {ID: 12, Version: 1, ModificationTime: \"0,0\", State: OFFLINE, OfflineReason: \"foo\"}",
		},
		{
			desc: NewCreatedMutable(descpb.DatabaseDescriptor{
				ID:            42,
				Version:       2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "bar",
			}),
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
		desc *Immutable
	}{
		{`invalid database ID 0`,
			NewImmutable(descpb.DatabaseDescriptor{
				Name:       "db",
				ID:         0,
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			}),
		},
		{
			`region "us-east-1" seen twice on db 200`,
			NewImmutable(descpb.DatabaseDescriptor{
				Name: "multi-region-db",
				ID:   200,
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					Regions: []descpb.DatabaseDescriptor_RegionConfig_Region{
						{Name: "us-east-1"},
						{Name: "us-east-1"},
					},
					PrimaryRegion: "us-east-1",
				},
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			}),
		},
		{
			`primary region unset on a multi-region db 200`,
			NewImmutable(descpb.DatabaseDescriptor{
				Name: "multi-region-db",
				ID:   200,
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					Regions: []descpb.DatabaseDescriptor_RegionConfig_Region{
						{Name: "us-east-1"},
					},
				},
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			}),
		},
		{
			`primary region not found in list of regions on db 200`,
			NewImmutable(descpb.DatabaseDescriptor{
				Name: "multi-region-db",
				ID:   200,
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					Regions: []descpb.DatabaseDescriptor_RegionConfig_Region{
						{Name: "us-east-1"},
					},
					PrimaryRegion: "us-east-2",
				},
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			}),
		},
	}
	for i, d := range testData {
		t.Run(d.err, func(t *testing.T) {
			expectedErr := fmt.Sprintf("%s %q (%d): %s", d.desc.TypeName(), d.desc.GetName(), d.desc.GetID(), d.err)
			if err := catalog.ValidateSelf(d.desc); err == nil {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, d.desc)
			} else if expectedErr != err.Error() {
				t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, expectedErr, err)
			}
		})
	}
}

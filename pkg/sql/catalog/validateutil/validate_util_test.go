// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validateutil_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/validateutil"
	"github.com/stretchr/testify/require"
)

type testNamespaceGetter struct {
	reads  []descpb.NameInfo
	values map[descpb.NameInfo]descpb.ID
}

func (t *testNamespaceGetter) GetNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID descpb.ID, name string,
) (descpb.ID, bool, error) {
	ni := descpb.NameInfo{
		ParentID:       parentID,
		ParentSchemaID: parentSchemaID,
		Name:           name,
	}
	t.reads = append(t.reads, ni)
	id, ok := t.values[ni]
	return id, ok, nil
}

var _ catalog.NamespaceGetter = (*testNamespaceGetter)(nil)

func TestValidateNamespace(t *testing.T) {
	for _, tc := range []struct {
		desc     catalog.Descriptor
		expReads []descpb.NameInfo
		values   map[descpb.NameInfo]descpb.ID
		expErrRE string
	}{
		{
			desc: tabledesc.NewImmutable(descpb.TableDescriptor{
				Name:                    "foo",
				ParentID:                1,
				UnexposedParentSchemaID: 29,
				ID:                      2,
			}),
			expReads: []descpb.NameInfo{
				{1, 29, "foo"},
			},
			values: map[descpb.NameInfo]descpb.ID{
				{1, 29, "foo"}: 2,
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			ns := testNamespaceGetter{values: tc.values}
			err := validateutil.ValidateNamespace(context.Background(), tc.desc, &ns)
			if tc.expErrRE == "" {
				require.NoError(t, err)
			} else {
				require.Regexp(t, tc.expErrRE, err)
			}
			require.ElementsMatch(t, tc.expReads, ns.reads)
		})
	}
}

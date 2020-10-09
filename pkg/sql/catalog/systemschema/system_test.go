// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemschema

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/stretchr/testify/require"
)

func TestShouldSplitAtDesc(t *testing.T) {
	for inner, should := range map[catalog.Descriptor]bool{
		tabledesc.NewImmutable(descpb.TableDescriptor{}):                                              true,
		tabledesc.NewImmutable(descpb.TableDescriptor{ViewQuery: "SELECT"}):                           false,
		tabledesc.NewImmutable(descpb.TableDescriptor{ViewQuery: "SELECT", IsMaterializedView: true}): true,
		dbdesc.NewInitial(42, "db", security.AdminRoleName()):                                         false,
		typedesc.NewCreatedMutable(descpb.TypeDescriptor{}):                                           false,
		schemadesc.NewImmutable(descpb.SchemaDescriptor{}):                                            false,
	} {
		var rawDesc roachpb.Value
		require.NoError(t, rawDesc.SetProto(inner.DescriptorProto()))
		require.Equal(t, should, ShouldSplitAtDesc(&rawDesc))
	}
}

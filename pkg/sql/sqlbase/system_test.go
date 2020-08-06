// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/stretchr/testify/require"
)

func TestShouldSplitAtDesc(t *testing.T) {
	for inner, should := range map[Descriptor]bool{
		NewImmutableTableDescriptor(descpb.TableDescriptor{}):                    true,
		NewImmutableTableDescriptor(descpb.TableDescriptor{ViewQuery: "SELECT"}): false,
		NewInitialDatabaseDescriptor(42, "db", security.AdminRole):               false,
		NewMutableCreatedTypeDescriptor(descpb.TypeDescriptor{}):                 false,
		NewImmutableSchemaDescriptor(descpb.SchemaDescriptor{}):                  false,
	} {
		var rawDesc roachpb.Value
		require.NoError(t, rawDesc.SetProto(inner.DescriptorProto()))
		require.Equal(t, should, ShouldSplitAtDesc(&rawDesc))
	}
}

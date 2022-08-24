// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

// TestDescriptorProtoString is to make sure gogo/protobuf is able to text
// marshal a protobuf struct has child field of type EnumMetadata
func TestDescriptorProtoString(t *testing.T) {
	enumMembers := []string{"hi", "hello"}
	enumType := types.MakeEnum(catid.TypeIDToOID(500), catid.TypeIDToOID(100500))
	enumType.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Schema: "test",
			Name:   "greeting",
		},
		EnumData: &types.EnumMetadata{
			LogicalRepresentations: enumMembers,
			PhysicalRepresentations: [][]byte{
				{0x42, 0x1},
				{0x42},
			},
			IsMemberReadOnly: make([]bool, len(enumMembers)),
		},
	}
	desc := &descpb.ColumnDescriptor{
		Name: "c",
		ID:   1,
		Type: enumType,
	}

	var str string
	require.NotPanics(t, func() { str = desc.String() })
	// Assert we only dump InternalType from types.T without metadata
	require.Contains(t, str, "type:<family")
	require.NotContains(t, str, "TypeMeta:<Name")
}

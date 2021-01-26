// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package schematestutils is a utility package for constructing schema objects
// in the context of cdc.
package schematestutils

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/gogo/protobuf/proto"
)

// MakeTableDesc makes a generic table descriptor with the provided properties.
func MakeTableDesc(
	tableID descpb.ID, version descpb.DescriptorVersion, modTime hlc.Timestamp, cols int,
) *tabledesc.Immutable {
	td := descpb.TableDescriptor{
		Name:             "foo",
		ID:               tableID,
		Version:          version,
		ModificationTime: modTime,
		NextColumnID:     1,
	}
	for i := 0; i < cols; i++ {
		td.Columns = append(td.Columns, *MakeColumnDesc(td.NextColumnID))
		td.NextColumnID++
	}
	return tabledesc.NewImmutable(td)
}

// MakeColumnDesc makes a generic column descriptor with the provided id.
func MakeColumnDesc(id descpb.ColumnID) *descpb.ColumnDescriptor {
	return &descpb.ColumnDescriptor{
		Name:        "c" + strconv.Itoa(int(id)),
		ID:          id,
		Type:        types.Bool,
		DefaultExpr: proto.String("true"),
	}
}

// AddColumnDropBackfillMutation adds a mutation to desc to drop a column.
// Yes, this does modify an Immutable.
func AddColumnDropBackfillMutation(desc *tabledesc.Immutable) *tabledesc.Immutable {
	desc.Mutations = append(desc.Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_DROP,
		Descriptor_: &descpb.DescriptorMutation_Column{Column: MakeColumnDesc(desc.NextColumnID - 1)},
	})
	return desc
}

// AddNewColumnBackfillMutation adds a mutation to desc to add a column.
// Yes, this does modify an Immutable.
func AddNewColumnBackfillMutation(desc *tabledesc.Immutable) *tabledesc.Immutable {
	desc.Mutations = append(desc.Mutations, descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Column{Column: MakeColumnDesc(desc.NextColumnID)},
		State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_ADD,
		MutationID:  0,
		Rollback:    false,
	})
	return desc
}

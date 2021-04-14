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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/gogo/protobuf/proto"
)

// MakeTableDesc makes a generic table descriptor with the provided properties.
func MakeTableDesc(
	tableID descpb.ID,
	version descpb.DescriptorVersion,
	modTime hlc.Timestamp,
	cols int,
	primaryKeyIndex int,
) catalog.TableDescriptor {
	td := descpb.TableDescriptor{
		Name:             "foo",
		ID:               tableID,
		Version:          version,
		ModificationTime: modTime,
		NextColumnID:     1,
		PrimaryIndex: descpb.IndexDescriptor{
			ID: descpb.IndexID(primaryKeyIndex),
		},
	}
	for i := 0; i < cols; i++ {
		td.Columns = append(td.Columns, *MakeColumnDesc(td.NextColumnID))
		td.NextColumnID++
	}
	return tabledesc.NewBuilder(&td).BuildImmutableTable()
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

// SetLocalityRegionalByRow sets the LocalityConfig of the table
// descriptor such that desc.IsLocalityRegionalByRow will return true.
func SetLocalityRegionalByRow(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().LocalityConfig = &descpb.TableDescriptor_LocalityConfig{
		Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByRow_{
			RegionalByRow: &descpb.TableDescriptor_LocalityConfig_RegionalByRow{},
		},
	}
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddColumnDropBackfillMutation adds a mutation to desc to drop a column.
// Yes, this does modify an immutable.
func AddColumnDropBackfillMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_DROP,
		Descriptor_: &descpb.DescriptorMutation_Column{Column: MakeColumnDesc(desc.GetNextColumnID() - 1)},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddNewColumnBackfillMutation adds a mutation to desc to add a column.
// Yes, this does modify an immutable.
func AddNewColumnBackfillMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Column{Column: MakeColumnDesc(desc.GetNextColumnID())},
		State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_ADD,
		MutationID:  0,
		Rollback:    false,
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddPrimaryKeySwapMutation adds a mutation to desc to do a primary key swap.
// Yes, this does modify an immutable.
func AddPrimaryKeySwapMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_ADD,
		Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{PrimaryKeySwap: &descpb.PrimaryKeySwap{}},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddNewIndexMutation adds a mutation to desc to add an index.
// Yes, this does modify an immutable.
func AddNewIndexMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_ADD,
		Descriptor_: &descpb.DescriptorMutation_Index{Index: &descpb.IndexDescriptor{}},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddDropIndexMutation adds a mutation to desc to drop an index.
// Yes, this does modify an immutable.
func AddDropIndexMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_DROP,
		Descriptor_: &descpb.DescriptorMutation_Index{Index: &descpb.IndexDescriptor{}},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// partitionByFromTableDesc constructs a PartitionBy clause from a table descriptor.
func partitionByFromTableDesc(
	codec keys.SQLCodec, tableDesc *tabledesc.Mutable,
) (*tree.PartitionBy, error) {
	idxDesc := tableDesc.GetPrimaryIndex().IndexDesc()
	partDesc := idxDesc.Partitioning
	return partitionByFromTableDescImpl(codec, tableDesc, idxDesc, &partDesc, 0)
}

func partitionByFromTableDescImpl(
	codec keys.SQLCodec,
	tableDesc *tabledesc.Mutable,
	idxDesc *descpb.IndexDescriptor,
	partDesc *descpb.PartitioningDescriptor,
	colOffset int,
) (*tree.PartitionBy, error) {
	if partDesc.NumColumns == 0 {
		return nil, nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	partitionBy := &tree.PartitionBy{
		Fields: make(tree.NameList, partDesc.NumColumns),
		List:   make([]tree.ListPartition, len(partDesc.List)),
		Range:  make([]tree.RangePartition, len(partDesc.Range)),
	}
	for i := 0; i < int(partDesc.NumColumns); i++ {
		partitionBy.Fields[i] = tree.Name(idxDesc.ColumnNames[colOffset+i])
	}

	a := &rowenc.DatumAlloc{}
	for i := range partDesc.List {
		part := &partDesc.List[i]
		partitionBy.List[i].Name = tree.UnrestrictedName(part.Name)
		partitionBy.List[i].Exprs = make(tree.Exprs, len(part.Values))
		for j, values := range part.Values {
			tuple, _, err := rowenc.DecodePartitionTuple(
				a,
				codec,
				tableDesc,
				idxDesc,
				partDesc,
				values,
				fakePrefixDatums,
			)
			if err != nil {
				return nil, err
			}
			exprs, err := partitionTupleToExprs(tuple)
			if err != nil {
				return nil, err
			}
			partitionBy.List[i].Exprs[j] = &tree.Tuple{
				Exprs: exprs,
			}
		}
		var err error
		if partitionBy.List[i].Subpartition, err = partitionByFromTableDescImpl(
			codec,
			tableDesc,
			idxDesc,
			&part.Subpartitioning,
			colOffset+int(partDesc.NumColumns),
		); err != nil {
			return nil, err
		}
	}
	for i, part := range partDesc.Range {
		partitionBy.Range[i].Name = tree.UnrestrictedName(part.Name)
		fromTuple, _, err := rowenc.DecodePartitionTuple(
			a,
			codec,
			tableDesc,
			idxDesc,
			partDesc,
			part.FromInclusive,
			fakePrefixDatums,
		)
		if err != nil {
			return nil, err
		}
		if partitionBy.Range[i].From, err = partitionTupleToExprs(fromTuple); err != nil {
			return nil, err
		}
		toTuple, _, err := rowenc.DecodePartitionTuple(
			a,
			codec,
			tableDesc,
			idxDesc,
			partDesc,
			part.ToExclusive,
			fakePrefixDatums,
		)
		if err != nil {
			return nil, err
		}
		if partitionBy.Range[i].To, err = partitionTupleToExprs(toTuple); err != nil {
			return nil, err
		}
	}
	return partitionBy, nil
}

func partitionTupleToExprs(t *rowenc.PartitionTuple) (tree.Exprs, error) {
	exprs := make(tree.Exprs, len(t.Datums)+t.SpecialCount)
	for i, d := range t.Datums {
		exprs[i] = d
	}
	for i := 0; i < t.SpecialCount; i++ {
		switch t.Special {
		case rowenc.PartitionDefaultVal:
			exprs[i+len(t.Datums)] = &tree.DefaultVal{}
		case rowenc.PartitionMinVal:
			exprs[i+len(t.Datums)] = &tree.PartitionMinVal{}
		case rowenc.PartitionMaxVal:
			exprs[i+len(t.Datums)] = &tree.PartitionMaxVal{}
		default:
			return nil, errors.AssertionFailedf("unknown special value found: %v", t.Special)
		}
	}
	return exprs, nil
}

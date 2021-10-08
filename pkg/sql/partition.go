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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// partitionByFromTableDesc constructs a PartitionBy clause from a table descriptor.
func partitionByFromTableDesc(
	codec keys.SQLCodec, tableDesc *tabledesc.Mutable,
) (*tree.PartitionBy, error) {
	idx := tableDesc.GetPrimaryIndex()
	return partitionByFromTableDescImpl(codec, tableDesc, idx, idx.GetPartitioning(), 0)
}

// partitionByFromTableDescImpl contains the inner logic of partitionByFromTableDesc.
// We derive the Fields, LIST and RANGE clauses from the table descriptor, recursing
// into the subpartitions as required for LIST partitions.
func partitionByFromTableDescImpl(
	codec keys.SQLCodec,
	tableDesc *tabledesc.Mutable,
	idx catalog.Index,
	part catalog.Partitioning,
	colOffset int,
) (*tree.PartitionBy, error) {
	if part.NumColumns() == 0 {
		return nil, nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	partitionBy := &tree.PartitionBy{
		Fields: make(tree.NameList, part.NumColumns()),
		List:   make([]tree.ListPartition, 0, part.NumLists()),
		Range:  make([]tree.RangePartition, 0, part.NumRanges()),
	}
	for i := 0; i < part.NumColumns(); i++ {
		partitionBy.Fields[i] = tree.Name(idx.GetKeyColumnName(colOffset + i))
	}

	// Copy the LIST of the PARTITION BY clause.
	a := &rowenc.DatumAlloc{}
	err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) (err error) {
		lp := tree.ListPartition{
			Name:  tree.UnrestrictedName(name),
			Exprs: make(tree.Exprs, len(values)),
		}
		for j, values := range values {
			tuple, _, err := rowenc.DecodePartitionTuple(
				a,
				codec,
				tableDesc,
				idx,
				part,
				values,
				fakePrefixDatums,
			)
			if err != nil {
				return err
			}
			exprs, err := partitionTupleToExprs(tuple)
			if err != nil {
				return err
			}
			lp.Exprs[j] = &tree.Tuple{
				Exprs: exprs,
			}
		}
		lp.Subpartition, err = partitionByFromTableDescImpl(
			codec,
			tableDesc,
			idx,
			subPartitioning,
			colOffset+part.NumColumns(),
		)
		partitionBy.List = append(partitionBy.List, lp)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Copy the RANGE of the PARTITION BY clause.
	err = part.ForEachRange(func(name string, from, to []byte) error {
		rp := tree.RangePartition{Name: tree.UnrestrictedName(name)}
		fromTuple, _, err := rowenc.DecodePartitionTuple(
			a, codec, tableDesc, idx, part, from, fakePrefixDatums)
		if err != nil {
			return err
		}
		rp.From, err = partitionTupleToExprs(fromTuple)
		if err != nil {
			return err
		}
		toTuple, _, err := rowenc.DecodePartitionTuple(
			a, codec, tableDesc, idx, part, to, fakePrefixDatums)
		if err != nil {
			return err
		}
		rp.To, err = partitionTupleToExprs(toTuple)
		partitionBy.Range = append(partitionBy.Range, rp)
		return err
	})
	if err != nil {
		return nil, err
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

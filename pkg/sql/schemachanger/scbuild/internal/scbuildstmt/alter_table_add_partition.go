// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
)

func alterTableAddPartition(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableAddPartition,
) {
	// Get the primary index for the table.
	primaryIndexID := getCurrentPrimaryIndexID(b, tbl.TableID)

	// Check that the table is partitioned and find the partitioning descriptor.
	var indexPartitioning *scpb.IndexPartitioning
	b.QueryByID(tbl.TableID).FilterIndexPartitioning().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID == primaryIndexID {
				indexPartitioning = e
			}
		})

	if indexPartitioning == nil {
		panic(errors.WithHint(
			pgerror.Newf(pgcode.InvalidTableDefinition,
				"table %q is not partitioned", tn.Object()),
			"Use PARTITION BY RANGE when creating the table."))
	}

	// Validate that FROM expressions < TO expressions for the new partition.
	if len(t.Partition.From) == 0 || len(t.Partition.To) == 0 {
		panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
			"PARTITION %s: both FROM and TO must be specified",
			t.Partition.Name))
	}

	// Query for existing partitions and check for duplicates.
	// Use Normalize() for case-insensitive comparison.
	newPartitionName := t.Partition.Name.Normalize()
	partition := tabledesc.NewPartitioning(&indexPartitioning.PartitioningDescriptor)
	_ = partition.ForEachPartitionName(func(name string) error {
		if name == newPartitionName {
			// TODO(spilchen): we hard code primary here. But we need to think about
			// this command wrt secondary indexes as well.
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
				"PARTITION %s: name must be unique (used twice in index %q)",
				t.Partition.Name, "primary"))
		}
		return nil
	})

	// Get the partitioning columns to encode the new partition bounds.
	// Find the columns used for partitioning.
	keyColumns, _, _ := getSortedColumnIDsInIndexByKind(b, tbl.TableID, primaryIndexID)
	var cols []*scpb.ColumnName
	for i := 0; i < partition.NumColumns(); i++ {
		colName := b.QueryByID(tbl.TableID).FilterColumnName().
			Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnName) bool {
				return e.ColumnID == keyColumns[i]
			}).MustGetOneElement()
		cols = append(cols, colName)
	}

	// Encode the new partition's FROM and TO bounds.
	newFromEncoded, err := valueEncodePartitionTuple(
		b, tbl.TableID, tree.PartitionByRange, &tree.Tuple{Exprs: t.Partition.From}, cols,
	)
	if err != nil {
		panic(errors.Wrapf(err, "PARTITION %s", t.Partition.Name))
	}

	newToEncoded, err := valueEncodePartitionTuple(
		b, tbl.TableID, tree.PartitionByRange, &tree.Tuple{Exprs: t.Partition.To}, cols,
	)
	if err != nil {
		panic(errors.Wrapf(err, "PARTITION %s", t.Partition.Name))
	}

	// Decode the encoded bounds to get keys for validation.
	a := &tree.DatumAlloc{}
	fakePrefixDatums := make([]tree.Datum, 0)

	newFromTuple, newFromKey, err := decodePartitionTuple(
		b, a, tbl.TableID, primaryIndexID, partition, newFromEncoded, fakePrefixDatums,
	)
	if err != nil {
		panic(errors.Wrapf(err, "PARTITION %s", t.Partition.Name))
	}

	newToTuple, newToKey, err := decodePartitionTuple(
		b, a, tbl.TableID, primaryIndexID, partition, newToEncoded, fakePrefixDatums,
	)
	if err != nil {
		panic(errors.Wrapf(err, "PARTITION %s", t.Partition.Name))
	}

	// Build an interval tree with existing partitions to check for overlaps.
	tree := interval.NewTree(interval.ExclusiveOverlapper)
	err = partition.ForEachRange(func(name string, from, to []byte) error {
		fromTuple, fromKey, err := decodePartitionTuple(
			b, a, tbl.TableID, primaryIndexID, partition, from, fakePrefixDatums)
		if err != nil {
			return errors.Wrapf(err, "PARTITION %s", name)
		}
		toTuple, toKey, err := decodePartitionTuple(
			b, a, tbl.TableID, primaryIndexID, partition, to, fakePrefixDatums)
		if err != nil {
			return errors.Wrapf(err, "PARTITION %s", name)
		}
		pi := partitionInterval{name, fromKey, toKey}
		if err := tree.Insert(pi, false /* fast */); errors.Is(err, interval.ErrEmptyRange) {
			return pgerror.Newf(pgcode.InvalidObjectDefinition,
				"PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
				name, fromTuple, toTuple)
		} else if errors.Is(err, interval.ErrInvertedRange) {
			return pgerror.Newf(pgcode.InvalidObjectDefinition,
				"PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
				name, fromTuple, toTuple)
		} else if err != nil {
			return errors.Wrapf(err, "PARTITION %s", name)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Check if the new partition has valid bounds (FROM < TO).
	newPI := partitionInterval{newPartitionName, newFromKey, newToKey}
	if err := tree.Insert(newPI, false /* fast */); errors.Is(err, interval.ErrEmptyRange) {
		panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
			"PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
			t.Partition.Name, newFromTuple, newToTuple))
	} else if errors.Is(err, interval.ErrInvertedRange) {
		panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
			"PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
			t.Partition.Name, newFromTuple, newToTuple))
	} else if err != nil {
		panic(errors.Wrapf(err, "PARTITION %s", t.Partition.Name))
	}

	// Check for overlaps with existing partitions.
	if overlaps := tree.Get(newPI.Range()); len(overlaps) > 1 {
		// len(overlaps) > 1 because the interval tree will include the newly inserted partition itself.
		// We want to find if there are any OTHER overlapping partitions.
		for _, overlap := range overlaps {
			existingPartition := overlap.(partitionInterval)
			if existingPartition.name != newPartitionName {
				panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
					"partitions %s and %s overlap",
					existingPartition.name, newPartitionName))
			}
		}
	}

	// No-op for now: Future implementation will handle actually adding the partition.
	// This validation ensures the new partition would be valid if we were to add it.
}

// partitionInterval implements interval.Interface for partition range checking.
type partitionInterval struct {
	name  string
	start roachpb.Key
	end   roachpb.Key
}

var _ interval.Interface = partitionInterval{}

// ID is part of interval.Interface but unused in partition validation.
func (ps partitionInterval) ID() uintptr { return 0 }

// Range is part of interval.Interface.
func (ps partitionInterval) Range() interval.Range {
	return interval.Range{Start: []byte(ps.start), End: []byte(ps.end)}
}

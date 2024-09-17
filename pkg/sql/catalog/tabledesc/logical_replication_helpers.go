// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

const cannotLDRMsg = "cannot create logical replication stream"

// CheckLogicalReplicationCompatibility verifies that the destination table
// descriptor is a valid target for logical replication from the source table.
func CheckLogicalReplicationCompatibility(src, dst *descpb.TableDescriptor) error {
	if err := checkSrcDstColsMatch(src, dst); err != nil {
		return err
	}

	if err := checkColumnFamilies(dst); err != nil {
		return err
	}

	if err := checkCompositeTypesInPrimaryKey(dst); err != nil {
		return err
	}

	if err := checkVirtualColumns(dst); err != nil {
		return err
	}

	return nil
}

// We disallow expression evaluation (e.g. virtual columns that are stored in an
// index) because the LDR KV write path does not understand how to evaluate
// expressions. The writer expects to receive the full set of columns, even the
// computed ones, along with a list of columns that we've already determined
// should be updated.
func checkVirtualColumns(dst *descpb.TableDescriptor) error {
	// Disallow partial indexes.
	for _, idx := range dst.Indexes {
		if idx.IsPartial() {
			return errors.WithMessage(pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"table %s has a partial index %s", dst.Name, idx.Name,
			), cannotLDRMsg)
		}
	}

	// Disallow virtual columns if they are a key of an index.
	// NB: it is impossible for a virtual column to be stored in an index.
	for _, col := range dst.Columns {
		if col.IsComputed() && col.Virtual {
			for _, pkColID := range dst.PrimaryIndex.KeyColumnIDs {
				if pkColID == col.ID {
					return errors.WithMessage(pgerror.Newf(
						pgcode.InvalidTableDefinition,
						"table %s has a virtual computed column %s that appears in the primary key",
						dst.Name, col.Name,
					), cannotLDRMsg)
				}
			}
			for _, idx := range dst.Indexes {
				for _, keyColID := range idx.KeyColumnIDs {
					if keyColID == col.ID {
						return errors.WithMessage(pgerror.Newf(
							pgcode.InvalidTableDefinition,
							"table %s has a virtual computed column %s that is a key of index %s",
							dst.Name, col.Name, idx.Name,
						), cannotLDRMsg)
					}
				}
			}
		}
	}
	return nil
}

// Decoding a primary key with a composite type requires reading the current
// value. When the rangefeed sends over a delete, however, we do not see the
// current value. While we could rely on the prev value sent over the rangefeed,
// we currently have no way to handle phantom deletes (i.e. a delete on a key
// with no previous value)
func checkCompositeTypesInPrimaryKey(dst *descpb.TableDescriptor) error {
	for _, compositeColID := range dst.PrimaryIndex.CompositeColumnIDs {
		for _, keyColID := range dst.PrimaryIndex.KeyColumnIDs {
			if compositeColID == keyColID {
				colName := ""
				for _, col := range dst.Columns {
					if col.ID == keyColID {
						colName = col.Name
					}
				}
				return errors.WithMessage(pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"table %s has a primary key column (%s) with composite encoding",
					dst.Name, colName,
				), cannotLDRMsg)
			}
		}
	}
	return nil
}

// Replication does not work if a column in a family has a not null
// constraint. Even if all columns are nullable, it is very hard to
// differentiate between a delete and an update which nils out values in a
// given column family.
func checkColumnFamilies(dst *descpb.TableDescriptor) error {
	if len(dst.Families) > 1 {
		return errors.WithMessage(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"table %s has more than one column family", dst.Name,
		), cannotLDRMsg)
	}
	return nil
}

// All column names and types must match with the source table’s columns. The KV
// and SQL write path ingestion side logic assumes that src and dst columns
// match. If they don’t, the LDR job will DLQ these rows and move on.
func checkSrcDstColsMatch(src *descpb.TableDescriptor, dst *descpb.TableDescriptor) error {
	if len(src.Columns) != len(dst.Columns) {
		return errors.WithMessage(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"destination table %s has %d columns, but the source table %s has %d columns",
			dst.Name, len(dst.Columns), src.Name, len(src.Columns),
		), cannotLDRMsg)
	}
	for i := range src.Columns {
		srcCol := src.Columns[i]
		dstCol := dst.Columns[i]

		if srcCol.Name != dstCol.Name {
			return errors.WithMessage(pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"destination table %s column %s at position %d does not match source table %s column %s",
				dst.Name, dstCol.Name, i, src.Name, srcCol.Name,
			), cannotLDRMsg)
		}

		if dstCol.Type.UserDefined() {
			return errors.WithMessage(pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"destination table %s column %s has user-defined type %s",
				dst.Name, dstCol.Name, dstCol.Type.SQLStringForError(),
			), cannotLDRMsg)
		}

		if !srcCol.Type.Identical(dstCol.Type) {
			return errors.WithMessage(pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"destination table %s column %s has type %s, but the source table %s has type %s",
				dst.Name, dstCol.Name, dstCol.Type.SQLStringForError(), src.Name, srcCol.Type.SQLStringForError(),
			), cannotLDRMsg)
		}
	}
	return nil
}

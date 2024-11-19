// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package evalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// EncodeTableIndexKey is part of eval.CatalogBuiltins.
func (ec *Builtins) EncodeTableIndexKey(
	ctx context.Context,
	tableID catid.DescID,
	indexID catid.IndexID,
	rowDatums *tree.DTuple,
	performCast func(context.Context, tree.Datum, *types.T) (tree.Datum, error),
) ([]byte, error) {
	// Get the referenced table and index.
	tableDesc, err := ec.dc.ByIDWithLeased(ec.txn).WithoutNonPublic().Get().Table(ctx, tableID)
	if err != nil {
		return nil, err
	}
	index, err := catalog.MustFindIndexByID(tableDesc, indexID)
	if err != nil {
		return nil, err
	}
	// Collect the index columns. If the index is a non-unique secondary
	// index, it might have some extra key columns.
	indexColIDs := make([]descpb.ColumnID, index.NumKeyColumns(), index.NumKeyColumns()+index.NumKeySuffixColumns())
	for i := 0; i < index.NumKeyColumns(); i++ {
		indexColIDs[i] = index.GetKeyColumnID(i)
	}
	if index.GetID() != tableDesc.GetPrimaryIndexID() && !index.IsUnique() {
		for i := 0; i < index.NumKeySuffixColumns(); i++ {
			indexColIDs = append(indexColIDs, index.GetKeySuffixColumnID(i))
		}
	}

	// Ensure that the input tuple length equals the number of index cols.
	if len(rowDatums.D) != len(indexColIDs) {
		err := pgerror.Newf(
			pgcode.InvalidParameterValue,
			"number of values must equal number of columns in index %q",
			index.GetName(),
		)
		// If the index has some extra key columns, then output an error
		// message with some extra information to explain the subtlety.
		if index.GetID() != tableDesc.GetPrimaryIndexID() && !index.IsUnique() && index.NumKeySuffixColumns() > 0 {
			var extraColNames []string
			for i := 0; i < index.NumKeySuffixColumns(); i++ {
				id := index.GetKeySuffixColumnID(i)
				col, colErr := catalog.MustFindColumnByID(tableDesc, id)
				if colErr != nil {
					return nil, errors.CombineErrors(err, colErr)
				}
				extraColNames = append(extraColNames, col.GetName())
			}
			var allColNames []string
			for _, id := range indexColIDs {
				col, colErr := catalog.MustFindColumnByID(tableDesc, id)
				if colErr != nil {
					return nil, errors.CombineErrors(err, colErr)
				}
				allColNames = append(allColNames, col.GetName())
			}
			return nil, errors.WithHintf(
				err,
				"columns %v are implicitly part of index %q's key, include columns %v in this order",
				extraColNames,
				index.GetName(),
				allColNames,
			)
		}
		return nil, err
	}

	// Check that the input datums are typed as the index columns types.
	var datums tree.Datums
	for i, d := range rowDatums.D {
		// We perform a cast here rather than a type check because datums
		// already have a fixed type, and not enough information is known at
		// typechecking time to ensure that the datums are typed with the
		// types of the index columns. So, try to cast the input datums to
		// the types of the index columns here.
		var newDatum tree.Datum
		col, err := catalog.MustFindColumnByID(tableDesc, indexColIDs[i])
		if err != nil {
			return nil, err
		}
		if d.ResolvedType().Family() == types.UnknownFamily {
			if !col.IsNullable() {
				return nil, pgerror.Newf(pgcode.NotNullViolation, "NULL provided as a value for a nonnullable column")
			}
			newDatum = tree.DNull
		} else {
			expectedTyp := col.GetType()
			newDatum, err = performCast(ctx, d, expectedTyp)
			if err != nil {
				return nil, errors.WithHint(err, "try to explicitly cast each value to the corresponding column type")
			}
		}
		datums = append(datums, newDatum)
	}

	// Create a column id to row index map. In this case, each column ID
	// just maps to the i'th ordinal.
	var colMap catalog.TableColMap
	for i, id := range indexColIDs {
		colMap.Set(id, i)
	}
	// Finally, encode the index key using the provided datums.
	keyPrefix := rowenc.MakeIndexKeyPrefix(ec.codec, tableDesc.GetID(), index.GetID())
	keyAndSuffixCols := tableDesc.IndexFetchSpecKeyAndSuffixColumns(index)
	if len(datums) > len(keyAndSuffixCols) {
		return nil, errors.Errorf("encoding too many columns (%d)", len(datums))
	}
	res, _, err := rowenc.EncodePartialIndexKey(keyAndSuffixCols[:len(datums)], colMap, datums, keyPrefix)
	return res, err
}

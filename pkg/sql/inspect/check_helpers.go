// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/spanutils"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func loadTableDesc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableID descpb.ID,
	tableVersion descpb.DescriptorVersion,
	asOf hlc.Timestamp,
) (catalog.TableDescriptor, error) {
	var tableDesc catalog.TableDescriptor
	if err := execCfg.DistSQLSrv.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if !asOf.IsEmpty() {
			if err := txn.KV().SetFixedTimestamp(ctx, asOf); err != nil {
				return err
			}
		}

		byIDGetter := txn.Descriptors().ByIDWithLeased(txn.KV())
		if !asOf.IsEmpty() {
			byIDGetter = txn.Descriptors().ByIDWithoutLeased(txn.KV())
		}

		var err error
		tableDesc, err = byIDGetter.WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return err
		}
		if tableVersion != 0 && tableDesc.GetVersion() != tableVersion {
			return errors.WithHintf(
				errors.Newf(
					"table %s [%d] has had a schema change since the job has started at %s",
					tableDesc.GetName(),
					tableDesc.GetID(),
					tableDesc.GetModificationTime().GoTime().Format(time.RFC3339),
				),
				"use AS OF SYSTEM TIME to avoid schema changes during inspection",
			)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return tableDesc, nil
}

func findIndexByID(
	tableDesc catalog.TableDescriptor, indexID descpb.IndexID,
) (catalog.Index, error) {
	for _, idx := range tableDesc.AllIndexes() {
		if idx.GetID() == indexID {
			return idx, nil
		}
	}
	return nil, errors.AssertionFailedf("no index with ID %d found in table %d", indexID, tableDesc.GetID())
}

// encodeColumnName properly encodes a column name for use in SQL.
func encodeColumnName(columnName string) string {
	var buf bytes.Buffer
	lexbase.EncodeRestrictedSQLIdent(&buf, columnName, lexbase.EncNoFlags)
	return buf.String()
}

// extractRegionFromSpan extracts the value of the leading (region) column from
// a span on a regional by row table.
func extractRegionFromSpan(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	span roachpb.Span,
	tableDesc catalog.TableDescriptor,
) (string, error) {
	if !tableDesc.IsLocalityRegionalByRow() {
		if !buildutil.CrdbTestBuild && !testing.Testing() { // For ease of testing, allow faked multi-region databases.
			return "", errors.AssertionFailedf("table is not REGIONAL BY ROW, cannot extract region from span")
		}
	}

	priIndex := tableDesc.GetPrimaryIndex()

	indexPrefix := cfg.Codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(priIndex.GetID()))

	if len(span.Key) <= len(indexPrefix) {
		return "", errors.Newf("span does not contain region column data")
	}

	regionColID := priIndex.GetKeyColumnID(0)
	regionCol, err := catalog.MustFindColumnByID(tableDesc, regionColID)
	if err != nil {
		return "", err
	}

	// Decode the region value from the span key.
	remainingKey := span.Key[len(indexPrefix):]
	var regionDatum tree.Datum
	regionDatum, _, err = keyside.Decode(
		nil, /* alloc */
		regionCol.GetType(),
		remainingKey,
		encoding.Ascending,
	)
	if err != nil {
		return "", errors.Wrap(err, "decoding region from span")
	}

	regionEnum, ok := regionDatum.(*tree.DEnum)
	if !ok {
		return "", errors.Newf("region column is not an enum type")
	}

	return regionEnum.LogicalRep, nil
}

// spanForRegion returns an equivalent span with the region column set to the specific region.
func spanForRegion(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	span roachpb.Span,
	tableDesc catalog.TableDescriptor,
	region string,
) (roachpb.Span, error) {
	priIndex := tableDesc.GetPrimaryIndex()

	indexPrefix := cfg.Codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(priIndex.GetID()))

	if len(span.Key) <= len(indexPrefix) {
		return roachpb.Span{}, errors.AssertionFailedf("span.Key is missing index prefix")
	}
	if len(span.EndKey) <= len(indexPrefix) {
		return roachpb.Span{}, errors.AssertionFailedf("span.EndKey is missing index prefix")
	}

	regionColID := priIndex.GetKeyColumnID(0)
	regionCol, err := catalog.MustFindColumnByID(tableDesc, regionColID)
	if err != nil {
		return roachpb.Span{}, err
	}

	regionDatum, err := tree.MakeDEnumFromLogicalRepresentation(regionCol.GetType(), region)
	if err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "invalid region %q", region)
	}

	regionPrefixBytes, err := keyside.Encode(indexPrefix, &regionDatum, encoding.Ascending)
	if err != nil {
		return roachpb.Span{}, errors.Wrap(err, "encoding region")
	}
	regionPrefix := roachpb.Key(regionPrefixBytes)

	startKeyAfterIndex := span.Key[len(indexPrefix):]
	endKeyAfterIndex := span.EndKey[len(indexPrefix):]

	// Decode and skip the region column from the original span
	_, startSuffix, err := keyside.Decode(
		nil, /* alloc */
		regionCol.GetType(),
		startKeyAfterIndex,
		encoding.Ascending,
	)
	if err != nil {
		return roachpb.Span{}, errors.Wrap(err, "decoding region from start key")
	}

	_, endSuffix, err := keyside.Decode(
		nil, /* alloc */
		regionCol.GetType(),
		endKeyAfterIndex,
		encoding.Ascending,
	)
	if err != nil {
		return roachpb.Span{}, errors.Wrap(err, "decoding region from end key")
	}

	key := append(slices.Clone(regionPrefix), startSuffix...)
	endKey := append(slices.Clone(regionPrefix), endSuffix...)
	return roachpb.Span{
		Key:    key,
		EndKey: endKey,
	}, nil
}

// getPredicateAndQueryArgs generates query bounds from the span to limit the query to the specified range.
// When no rows are found in a span, an empty predicate is returned.
func getPredicateAndQueryArgs(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	span roachpb.Span,
	tableDesc catalog.TableDescriptor,
	priIndex catalog.Index,
	asOf hlc.Timestamp,
	pkColNames []string,
	endPlaceholderOffset int,
) (predicate string, queryArgs []interface{}, err error) {
	// Assert that we get meaningful spans
	if span.Key.Equal(span.EndKey) || len(span.Key) == 0 || len(span.EndKey) == 0 {
		return "", nil, errors.AssertionFailedf("received invalid span: Key=%x EndKey=%x", span.Key, span.EndKey)
	}

	// Get primary key metadata for span conversion
	pkColTypes, err := spanutils.GetPKColumnTypes(tableDesc, priIndex.IndexDesc())
	if err != nil {
		return "", nil, errors.Wrap(err, "getting primary key column types")
	}

	pkColDirs := make([]catenumpb.IndexColumn_Direction, priIndex.NumKeyColumns())
	pkColIDs := catalog.TableColMap{}
	for i := 0; i < priIndex.NumKeyColumns(); i++ {
		colID := priIndex.GetKeyColumnID(i)
		pkColIDs.Set(colID, i)
		pkColDirs[i] = priIndex.GetKeyColumnDirection(i)
	}

	// Convert span to query bounds
	alloc := &tree.DatumAlloc{}
	bounds, hasRows, err := spanutils.SpanToQueryBounds(
		ctx, cfg.DB.KV(), cfg.Codec, pkColIDs, pkColTypes, pkColDirs,
		len(tableDesc.GetFamilies()), span, alloc, asOf,
	)
	if err != nil {
		return "", nil, errors.Wrap(err, "converting span to query bounds")
	}

	// If no rows exist in the primary index span, return an empty predicate
	// and no query arguments. Callers decide how to handle this case.
	if !hasRows {
		// Use empty predicate and no query arguments
		predicate = ""
		queryArgs = []interface{}{}
	} else {
		if len(bounds.Start) == 0 || len(bounds.End) == 0 {
			return "", nil, errors.AssertionFailedf("query bounds from span didn't produce start or end: %+v", bounds)
		}

		// Generate SQL predicate from the bounds
		// Encode column names for SQL usage
		encodedPkColNames := make([]string, len(pkColNames))
		for i, colName := range pkColNames {
			encodedPkColNames[i] = encodeColumnName(colName)
		}
		predicate, err = spanutils.RenderQueryBounds(encodedPkColNames, pkColDirs, pkColTypes, len(bounds.Start), len(bounds.End), true, endPlaceholderOffset)
		if err != nil {
			return "", nil, errors.Wrap(err, "rendering query bounds")
		}

		if strings.TrimSpace(predicate) == "" {
			return "", nil, errors.AssertionFailedf("query bounds from span didn't produce predicate: %+v", bounds)
		}

		// Prepare query arguments: end bounds first, then start bounds
		queryArgs = make([]interface{}, 0, len(bounds.End)+len(bounds.Start))
		for _, datum := range bounds.End {
			queryArgs = append(queryArgs, datum)
		}
		for _, datum := range bounds.Start {
			queryArgs = append(queryArgs, datum)
		}
	}

	return predicate, queryArgs, nil
}

// errorToInspectIssue converts internal errors to inspect issues so they can be
// captured and log data corruption or encoding errors as structured issues for
// investigation rather than percolated and failing the entire job.
func errorToInternalInspectIssue(
	err error,
	asOf hlc.Timestamp,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
	details map[redact.RedactableString]interface{},
) *inspectIssue {
	dets := make(map[redact.RedactableString]interface{})
	for k, v := range details {
		dets[k] = v
	}

	dets["error_message"] = err.Error()
	dets["error_type"] = "internal_query_error"
	dets["index_name"] = index.GetName()

	return &inspectIssue{
		ErrorType:  InternalError,
		AOST:       asOf.GoTime(),
		DatabaseID: tableDesc.GetParentID(),
		SchemaID:   tableDesc.GetParentSchemaID(),
		ObjectID:   tableDesc.GetID(),
		Details:    dets,
	}
}

// findUniqueColIdxs returns the indexes of the unique columns in the index.
func findUniqueColIdxs(tableDesc catalog.TableDescriptor, index catalog.Index) ([]int, error) {
	var uniqueColIdxs []int
	for i := 0; i < index.NumKeyColumns(); i++ {
		colID := index.GetKeyColumnID(i)
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, err
		}
		if col.HasDefault() {
			switch col.GetDefaultExpr() {
			case "unique_rowid()", "unordered_unique_rowid()":
				uniqueColIdxs = append(uniqueColIdxs, i)
			}
		}

	}
	return uniqueColIdxs, nil
}

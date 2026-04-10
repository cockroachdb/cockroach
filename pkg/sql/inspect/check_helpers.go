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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/spanutils"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	if !isRegionalByRow(tableDesc) {
		return "", errors.AssertionFailedf("table is not REGIONAL BY ROW, cannot extract region from span")
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

// spanForRegion returns an equivalent span with the region column set to the
// specific region.
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

	regionSpan, err := spanForFullRegion(cfg, tableDesc, region)
	if err != nil {
		return roachpb.Span{}, err
	}
	regionPrefix := regionSpan.Key

	startKeyAfterIndex := span.Key[len(indexPrefix):]
	endKeyAfterIndex := span.EndKey[len(indexPrefix):]

	// Decode and skip the region column from the original span
	startRegionDatum, startSuffix, err := keyside.Decode(
		nil, /* alloc */
		regionCol.GetType(),
		startKeyAfterIndex,
		encoding.Ascending,
	)
	if err != nil {
		return roachpb.Span{}, errors.Wrap(err, "decoding region from start key")
	}

	// If the original span uses the `PrefixEnd` of the region value, the updated
	// span does as well.
	startRegionEncoded, err := keyside.Encode(nil, startRegionDatum, encoding.Ascending)
	if err != nil {
		return roachpb.Span{}, errors.Wrap(err, "encoding start region")
	}

	if isPrefixEnd := bytes.Equal(endKeyAfterIndex, roachpb.Key(startRegionEncoded).PrefixEnd()); isPrefixEnd {
		key := append(slices.Clone(regionPrefix), startSuffix...)
		endKey := regionSpan.EndKey
		return roachpb.Span{
			Key:    key,
			EndKey: endKey,
		}, nil
	}

	// Decode the end key normally
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

// spanForFullRegion returns a span that covers the full region of a regional by
// row table.
func spanForFullRegion(
	cfg *execinfra.ServerConfig, tableDesc catalog.TableDescriptor, region string,
) (roachpb.Span, error) {
	priIndex := tableDesc.GetPrimaryIndex()

	indexPrefix := cfg.Codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(priIndex.GetID()))

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

	span := roachpb.Span{
		Key:    regionPrefix,
		EndKey: regionPrefix.PrefixEnd(),
	}

	return span, nil
}

// getPredicateAndQueryArgs generates query bounds from the span to limit the
// query to the specified range. When the span contains rows, the bounds are
// derived from the first and last row values. When it does not and
// needBoundsWhenEmpty is true, the bounds are derived directly from the span's
// Key and EndKey. hasRows indicates which case applied. When
// needBoundsWhenEmpty is false and the span is empty, an empty predicate is
// returned and hasRows is false.
func getPredicateAndQueryArgs(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	span roachpb.Span,
	tableDesc catalog.TableDescriptor,
	priIndex catalog.Index,
	asOf hlc.Timestamp,
	pkColNames []string,
	endPlaceholderOffset int,
	needBoundsWhenEmpty bool,
) (predicate string, queryArgs []interface{}, hasRows bool, err error) {
	// Assert that we get meaningful spans
	if span.Key.Equal(span.EndKey) || len(span.Key) == 0 || len(span.EndKey) == 0 {
		return "", nil, false, errors.AssertionFailedf("received invalid span: Key=%x EndKey=%x", span.Key, span.EndKey)
	}

	// Get primary key metadata for span conversion
	pkColTypes, err := spanutils.GetPKColumnTypes(tableDesc, priIndex.IndexDesc())
	if err != nil {
		return "", nil, false, errors.Wrap(err, "getting primary key column types")
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
		return "", nil, false, errors.Wrap(err, "converting span to query bounds")
	}

	encodedPkColNames := make([]string, len(pkColNames))
	for i, colName := range pkColNames {
		encodedPkColNames[i] = encodeColumnName(colName)
	}

	if !hasRows {
		if !needBoundsWhenEmpty {
			return "", nil, false, nil
		}

		// No rows exist in this span. Derive bounds directly from the span
		// keys so callers still receive a predicate scoped to the span. The
		// span.Key is inclusive and span.EndKey is exclusive, matching
		// roachpb.Span conventions.
		startDatums, err := decodeSpanKey(cfg.Codec, span.Key, pkColDirs, pkColTypes, alloc)
		if err != nil {
			return "", nil, false, errors.Wrap(err, "decoding span start key")
		}
		endDatums, err := decodeSpanKey(cfg.Codec, span.EndKey, pkColDirs, pkColTypes, alloc)
		if err != nil {
			return "", nil, false, errors.Wrap(err, "decoding span end key")
		}

		predicate, err = spanutils.RenderQueryBounds(
			encodedPkColNames, pkColDirs, pkColTypes,
			len(startDatums), len(endDatums),
			true /* startIncl */, endPlaceholderOffset,
		)
		if err != nil {
			return "", nil, false, errors.Wrap(err, "rendering span-key query bounds")
		}

		queryArgs = make([]interface{}, 0, len(endDatums)+len(startDatums))
		for _, datum := range endDatums {
			queryArgs = append(queryArgs, datum)
		}
		for _, datum := range startDatums {
			queryArgs = append(queryArgs, datum)
		}

		return predicate, queryArgs, false, nil
	}

	if len(bounds.Start) == 0 || len(bounds.End) == 0 {
		return "", nil, false, errors.AssertionFailedf("query bounds from span didn't produce start or end: %+v", bounds)
	}

	predicate, err = spanutils.RenderQueryBounds(
		encodedPkColNames, pkColDirs, pkColTypes,
		len(bounds.Start), len(bounds.End),
		true /* startIncl */, endPlaceholderOffset,
	)
	if err != nil {
		return "", nil, false, errors.Wrap(err, "rendering query bounds")
	}

	if strings.TrimSpace(predicate) == "" {
		return "", nil, false, errors.AssertionFailedf("query bounds from span didn't produce predicate: %+v", bounds)
	}

	// Prepare query arguments: end bounds first, then start bounds.
	queryArgs = make([]interface{}, 0, len(bounds.End)+len(bounds.Start))
	for _, datum := range bounds.End {
		queryArgs = append(queryArgs, datum)
	}
	for _, datum := range bounds.Start {
		queryArgs = append(queryArgs, datum)
	}

	return predicate, queryArgs, true, nil
}

// decodeSpanKey decodes a span boundary key into primary key datums. The key is
// expected to start with the tenant prefix followed by the table/index prefix
// and zero or more encoded PK column values. If the key contains only the
// table/index prefix (no column values), an empty datum slice is returned.
func decodeSpanKey(
	codec keys.SQLCodec,
	key roachpb.Key,
	pkColDirs []catenumpb.IndexColumn_Direction,
	pkColTypes []*types.T,
	alloc *tree.DatumAlloc,
) (tree.Datums, error) {
	remainder, err := codec.StripTenantPrefix(key)
	if err != nil {
		return nil, errors.Wrap(err, "stripping tenant prefix")
	}
	remainder, _, _, err = rowenc.DecodePartialTableIDIndexID(remainder)
	if err != nil {
		return nil, errors.Wrap(err, "decoding table/index ID")
	}
	if len(remainder) == 0 {
		return nil, nil
	}
	vals := make([]rowenc.EncDatum, len(pkColTypes))
	_, numVals, err := rowenc.DecodeKeyVals(vals, pkColDirs, remainder)
	if err != nil {
		return nil, errors.Wrap(err, "decoding key column values")
	}
	datums := make(tree.Datums, numVals)
	for i := 0; i < numVals; i++ {
		if err := vals[i].EnsureDecoded(pkColTypes[i], alloc); err != nil {
			return nil, errors.Wrapf(err, "decoding datum at position %d", i)
		}
		datums[i] = vals[i].Datum
	}
	return datums, nil
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

// findUniqueRowIDColPositions returns the indices of the unique row ID columns in the index.
func findUniqueRowIDColPositions(
	tableDesc catalog.TableDescriptor, index catalog.Index,
) ([]int, error) {
	var indices []int
	for i := 0; i < index.NumKeyColumns(); i++ {
		colID := index.GetKeyColumnID(i)
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, err
		}
		if col.HasDefault() {
			switch col.GetDefaultExpr() {
			case "unique_rowid()", "unordered_unique_rowid()":
				indices = append(indices, i)
			}
		}

	}
	return indices, nil
}

// getRegionsForTable retrieves the list of regions from an RBR table.
func getRegionsForTable(ctx context.Context, tableDesc catalog.TableDescriptor) ([]string, error) {
	if !isRegionalByRow(tableDesc) {
		return nil, errors.AssertionFailedf("table is not REGIONAL BY ROW")
	}

	regionColID := tableDesc.GetPrimaryIndex().GetKeyColumnID(0)
	regionCol, err := catalog.MustFindColumnByID(tableDesc, regionColID)
	if err != nil {
		return nil, err
	}

	regionType := regionCol.GetType()
	if regionType.TypeMeta.EnumData == nil {
		return nil, errors.AssertionFailedf("region column is not an enum type")
	}
	allRegions := regionType.TypeMeta.EnumData.LogicalRepresentations

	return allRegions, nil
}

func isTesting() bool {
	return buildutil.CrdbTestBuild || testing.Testing()
}

// isRegionalByRow checks whether a table is regional by row or if it's a mock
// RBR table run under test.
//
// Mocked RBR tables (in unit and base logic tests) allow for duplicated values
// in unique columns whereas the genuine articles (in CCL logic tests) are
// blocked by the uniqueness constraints. Single-node mock RBR tables are used
// to test the duplicated uniques case; multi-node RBR tables are used to test
// the unduplicated case.
func isRegionalByRow(table catalog.TableDescriptor) bool {
	priIndex := table.GetPrimaryIndex()

	isMockedRegionalByRow := isTesting() && priIndex.IndexDesc().KeyColumnNames[0] == tree.RegionalByRowRegionDefaultCol

	return table.IsLocalityRegionalByRow() || isMockedRegionalByRow
}

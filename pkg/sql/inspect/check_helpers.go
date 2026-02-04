// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/spanutils"
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

// getPredicateAndQueryArgs generates query bounds from the span to limit the query to the specified range.
func getPredicateAndQueryArgs(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	span roachpb.Span,
	tableDesc catalog.TableDescriptor,
	priIndex catalog.Index,
	asOf hlc.Timestamp,
	pkColNames []string,
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

	// If no rows exist in the primary index span, we still need to check for dangling
	// secondary index entries. We run the check with an empty predicate, which will
	// scan the entire secondary index within the span. Any secondary index entries found
	// will be dangling since there are no corresponding primary index rows.
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
		predicate, err = spanutils.RenderQueryBounds(
			encodedPkColNames, pkColDirs, pkColTypes,
			len(bounds.Start), len(bounds.End), true, 1,
		)
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

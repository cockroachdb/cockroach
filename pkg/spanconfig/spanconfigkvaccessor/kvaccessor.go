// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvaccessor

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// KVAccessor provides read/write access to all the span configurations for a
// CRDB cluster. It's a concrete implementation of the KVAccessor interface.
type KVAccessor struct {
	db *kv.DB
	ie sqlutil.InternalExecutor
	// optionalTxn captures the transaction we're scoped to; it's allowed to be
	// nil. If nil, it's unsafe to use multiple times as part of the same
	// request with any expectation of transactionality -- we're responsible for
	// opening a fresh txn.
	optionalTxn *kv.Txn
	settings    *cluster.Settings

	// configurationsTableFQN is typically 'system.public.span_configurations',
	// but left configurable for ease-of-testing.
	configurationsTableFQN string
}

var _ spanconfig.KVAccessor = &KVAccessor{}

// New constructs a new KVAccessor.
func New(
	db *kv.DB, ie sqlutil.InternalExecutor, settings *cluster.Settings, configurationsTableFQN string,
) *KVAccessor {
	if _, err := parser.ParseQualifiedTableName(configurationsTableFQN); err != nil {
		panic(fmt.Sprintf("unabled to parse configurations table FQN: %s", configurationsTableFQN))
	}

	return newKVAccessor(db, ie, settings, configurationsTableFQN, nil /* optionalTxn */)
}

// WithTxn is part of the KVAccessor interface.
func (k *KVAccessor) WithTxn(ctx context.Context, txn *kv.Txn) spanconfig.KVAccessor {
	if k.optionalTxn != nil {
		log.Fatalf(ctx, "KVAccessor already scoped to txn (was .WithTxn(...) chained multiple times?)")
	}
	return newKVAccessor(k.db, k.ie, k.settings, k.configurationsTableFQN, txn)
}

// GetSpanConfigRecords is part of the KVAccessor interface.
func (k *KVAccessor) GetSpanConfigRecords(
	ctx context.Context, targets []spanconfig.Target,
) (records []spanconfig.Record, retErr error) {
	if len(targets) == 0 {
		return records, nil
	}
	if err := validateSpanTargets(targets); err != nil {
		return nil, err
	}

	getStmt, getQueryArgs := k.constructGetStmtAndArgs(targets)
	it, err := k.ie.QueryIteratorEx(ctx, "get-span-cfgs", k.optionalTxn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getStmt, getQueryArgs...,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			records, retErr = nil, errors.CombineErrors(retErr, closeErr)
		}
	}()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		span := roachpb.Span{
			Key:    []byte(*row[0].(*tree.DBytes)),
			EndKey: []byte(*row[1].(*tree.DBytes)),
		}
		var conf roachpb.SpanConfig
		if err := protoutil.Unmarshal(([]byte)(*row[2].(*tree.DBytes)), &conf); err != nil {
			return nil, err
		}

		records = append(records, spanconfig.Record{
			Target: spanconfig.DecodeTarget(span),
			Config: conf,
		})
	}
	if err != nil {
		return nil, err
	}
	return records, nil
}

// UpdateSpanConfigRecords is part of the KVAccessor interface.
func (k *KVAccessor) UpdateSpanConfigRecords(
	ctx context.Context, toDelete []spanconfig.Target, toUpsert []spanconfig.Record,
) error {
	if k.optionalTxn != nil {
		return k.updateSpanConfigRecordsWithTxn(ctx, toDelete, toUpsert, k.optionalTxn)
	}

	return k.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return k.updateSpanConfigRecordsWithTxn(ctx, toDelete, toUpsert, txn)
	})
}

func newKVAccessor(
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	settings *cluster.Settings,
	configurationsTableFQN string,
	optionalTxn *kv.Txn,
) *KVAccessor {
	return &KVAccessor{
		db:                     db,
		ie:                     ie,
		optionalTxn:            optionalTxn,
		settings:               settings,
		configurationsTableFQN: configurationsTableFQN,
	}
}

func (k *KVAccessor) updateSpanConfigRecordsWithTxn(
	ctx context.Context, toDelete []spanconfig.Target, toUpsert []spanconfig.Record, txn *kv.Txn,
) error {
	if txn == nil {
		log.Fatalf(ctx, "expected non-nil txn")
	}

	if err := validateUpdateArgs(toDelete, toUpsert); err != nil {
		return err
	}

	var deleteStmt string
	var deleteQueryArgs []interface{}
	if len(toDelete) > 0 {
		deleteStmt, deleteQueryArgs = k.constructDeleteStmtAndArgs(toDelete)
	}

	var upsertStmt, validationStmt string
	var upsertQueryArgs, validationQueryArgs []interface{}
	if len(toUpsert) > 0 {
		var err error
		upsertStmt, upsertQueryArgs, err = k.constructUpsertStmtAndArgs(toUpsert)
		if err != nil {
			return err
		}

		validationStmt, validationQueryArgs = k.constructValidationStmtAndArgs(toUpsert)
	}

	if len(toDelete) > 0 {
		n, err := k.ie.ExecEx(ctx, "delete-span-cfgs", txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			deleteStmt, deleteQueryArgs...,
		)
		if err != nil {
			return err
		}
		if n != len(toDelete) {
			return errors.AssertionFailedf("expected to delete %d row(s), deleted %d", len(toDelete), n)
		}
	}

	if len(toUpsert) == 0 {
		// Nothing left to do
		return nil
	}

	if n, err := k.ie.ExecEx(ctx, "upsert-span-cfgs", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		upsertStmt, upsertQueryArgs...,
	); err != nil {
		return err
	} else if n != len(toUpsert) {
		return errors.AssertionFailedf("expected to upsert %d row(s), upserted %d", len(toUpsert), n)
	}

	if datums, err := k.ie.QueryRowEx(ctx, "validate-span-cfgs", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		validationStmt, validationQueryArgs...,
	); err != nil {
		return err
	} else if valid := bool(tree.MustBeDBool(datums[0])); !valid {
		return errors.AssertionFailedf("expected to find single row containing upserted spans")
	}

	return nil
}

// constructGetStmtAndArgs constructs the statement and query arguments needed
// to fetch span configs for the given spans.
func (k *KVAccessor) constructGetStmtAndArgs(targets []spanconfig.Target) (string, []interface{}) {
	// We want to fetch the overlapping span configs for each requested span in
	// a single round trip and using only constrained index scans. For a single
	// requested span, we effectively want to query the following:
	//
	//   -- to find spans overlapping with [$start, $end)
	//   SELECT * FROM system.span_configurations
	//    WHERE start_key < $end AND end_key > $start
	//
	// With the naive form above that translates to an unbounded index scan on
	// followed by a filter. We can do better by observing that start_key <
	// end_key, and that spans are non-overlapping.
	//
	//   SELECT * FROM span_configurations
	//    WHERE start_key >= $start AND start_key < $end
	//   UNION ALL
	//   SELECT * FROM (
	//     SELECT * FROM span_configurations
	//     WHERE start_key < $start ORDER BY start_key DESC LIMIT 1
	//   ) WHERE end_key > $start;
	//
	// The idea is to first find all spans that start within the query span, and
	// then to include the span with the start key immediately preceding the
	// query start if it also overlaps with the query span (achieved by
	// the outer filter). We're intentional about not pushing the filter down into
	// the query -- we want to select using only the start_key index. Doing so
	// results in an unbounded index scan [ - $start) with the filter and limit
	// applied after.
	//
	// To batch multiple query spans into the same statement, we make use of
	// UNION ALL.
	//
	//   ( ... statement above for 1st query span ...)
	//   UNION ALL
	//   ( ... statement above for 2nd query span ...)
	//   UNION ALL
	//   ...
	//
	var getStmtBuilder strings.Builder
	queryArgs := make([]interface{}, len(targets)*2)
	for i, target := range targets {
		if i > 0 {
			getStmtBuilder.WriteString(`UNION ALL`)
		}

		startKeyIdx, endKeyIdx := i*2, (i*2)+1
		encodedSp := target.Encode()
		queryArgs[startKeyIdx] = encodedSp.Key
		queryArgs[endKeyIdx] = encodedSp.EndKey

		fmt.Fprintf(&getStmtBuilder, `
SELECT start_key, end_key, config FROM %[1]s
 WHERE start_key >= $%[2]d AND start_key < $%[3]d
UNION ALL
SELECT start_key, end_key, config FROM (
  SELECT start_key, end_key, config FROM %[1]s
  WHERE start_key < $%[2]d ORDER BY start_key DESC LIMIT 1
) WHERE end_key > $%[2]d
`,
			k.configurationsTableFQN, // [1]
			startKeyIdx+1,            // [2] -- prepared statement placeholder (1-indexed)
			endKeyIdx+1,              // [3] -- prepared statement placeholder (1-indexed)
		)
	}
	return getStmtBuilder.String(), queryArgs
}

// constructDeleteStmtAndArgs constructs the statement and query arguments
// needed to delete span configs for the given spans.
func (k *KVAccessor) constructDeleteStmtAndArgs(
	toDelete []spanconfig.Target,
) (string, []interface{}) {
	// We're constructing a single delete statement to delete all requested
	// spans. It's of the form:
	//
	//   DELETE FROM span_configurations WHERE (start_key, end_key) IN
	//   (VALUES ( ... 1st span ... ), ( ... 2nd span ...), ... );
	//
	values := make([]string, len(toDelete))
	deleteQueryArgs := make([]interface{}, len(toDelete)*2)
	for i, toDel := range toDelete {
		startKeyIdx, endKeyIdx := i*2, (i*2)+1
		encodedSp := toDel.Encode()
		deleteQueryArgs[startKeyIdx] = encodedSp.Key
		deleteQueryArgs[endKeyIdx] = encodedSp.EndKey
		values[i] = fmt.Sprintf("($%d::BYTES, $%d::BYTES)",
			startKeyIdx+1, endKeyIdx+1) // prepared statement placeholders (1-indexed)
	}
	deleteStmt := fmt.Sprintf(`DELETE FROM %[1]s WHERE (start_key, end_key) IN (VALUES %[2]s)`,
		k.configurationsTableFQN, strings.Join(values, ", "))
	return deleteStmt, deleteQueryArgs
}

// constructUpsertStmtAndArgs constructs the statement and query arguments
// needed to upsert the given span config records.
func (k *KVAccessor) constructUpsertStmtAndArgs(
	toUpsert []spanconfig.Record,
) (string, []interface{}, error) {
	// We're constructing a single upsert statement to upsert all requested
	// spans. It's of the form:
	//
	//   UPSERT INTO span_configurations (start_key, end_key, config)
	//   VALUES ( ... 1st span ... ), ( ... 2nd span ...), ... ;
	//
	upsertValues := make([]string, len(toUpsert))
	upsertQueryArgs := make([]interface{}, len(toUpsert)*3)
	for i, record := range toUpsert {
		marshaled, err := protoutil.Marshal(&record.Config)
		if err != nil {
			return "", nil, err
		}

		startKeyIdx, endKeyIdx, configIdx := i*3, (i*3)+1, (i*3)+2
		upsertQueryArgs[startKeyIdx] = record.Target.Encode().Key
		upsertQueryArgs[endKeyIdx] = record.Target.Encode().EndKey
		upsertQueryArgs[configIdx] = marshaled
		upsertValues[i] = fmt.Sprintf("($%d::BYTES, $%d::BYTES, $%d::BYTES)",
			startKeyIdx+1, endKeyIdx+1, configIdx+1) // prepared statement placeholders (1-indexed)
	}
	upsertStmt := fmt.Sprintf(`UPSERT INTO %[1]s (start_key, end_key, config) VALUES %[2]s`,
		k.configurationsTableFQN, strings.Join(upsertValues, ", "))
	return upsertStmt, upsertQueryArgs, nil
}

// constructValidationStmtAndArgs constructs the statement and query arguments
// needed to validate that the spans being upserted don't violate table
// invariants (spans are non overlapping).
func (k *KVAccessor) constructValidationStmtAndArgs(
	toUpsert []spanconfig.Record,
) (string, []interface{}) {
	// We want to validate that upserting spans does not break the invariant
	// that spans in the table are non-overlapping. We only need to validate
	// the spans that are being upserted, and can use a query similar to
	// what we do in GetSpanConfigRecords. For a single upserted span, we
	// want effectively validate using:
	//
	//   SELECT count(*) = 1 FROM system.span_configurations
	//    WHERE start_key < $end AND end_key > $start
	//
	// Applying the GetSpanConfigRecords treatment, we can arrive at:
	//
	//   SELECT count(*) = 1 FROM (
	//    SELECT * FROM span_configurations
	//     WHERE start_key >= 100 AND start_key < 105
	//    UNION ALL
	//    SELECT * FROM (
	//      SELECT * FROM span_configurations
	//      WHERE start_key < 100 ORDER BY start_key DESC LIMIT 1
	//    ) WHERE end_key > 100
	//   )
	//
	// To batch multiple query spans into the same statement, we make use of
	// ALL and UNION ALL.
	//
	//   SELECT true = ALL(
	//     ( ... validation statement for 1st query span ...),
	//     UNION ALL
	//     ( ... validation statement for 2nd query span ...),
	//     ...
	//   )
	//
	var validationInnerStmtBuilder strings.Builder
	validationQueryArgs := make([]interface{}, len(toUpsert)*2)
	for i, entry := range toUpsert {
		if i > 0 {
			validationInnerStmtBuilder.WriteString(`UNION ALL`)
		}

		startKeyIdx, endKeyIdx := i*2, (i*2)+1
		validationQueryArgs[startKeyIdx] = entry.Target.Encode().Key
		validationQueryArgs[endKeyIdx] = entry.Target.Encode().EndKey

		fmt.Fprintf(&validationInnerStmtBuilder, `
SELECT count(*) = 1 FROM (
  SELECT start_key, end_key, config FROM %[1]s
   WHERE start_key >= $%[2]d AND start_key < $%[3]d
  UNION ALL
  SELECT start_key, end_key, config FROM (
    SELECT start_key, end_key, config FROM %[1]s
    WHERE start_key < $%[2]d ORDER BY start_key DESC LIMIT 1
  ) WHERE end_key > $%[2]d
)
`,
			k.configurationsTableFQN, // [1]
			startKeyIdx+1,            // [2] -- prepared statement placeholder (1-indexed)
			endKeyIdx+1,              // [3] -- prepared statement placeholder (1-indexed)
		)
	}
	validationStmt := fmt.Sprintf("SELECT true = ALL(%s)", validationInnerStmtBuilder.String())
	return validationStmt, validationQueryArgs
}

// validateUpdateArgs returns an error the arguments to UpdateSpanConfigRecords
// are malformed. All spans included in the toDelete and toUpsert list are
// expected to be valid and to have non-empty end keys. Spans are also expected
// to be non-overlapping with other spans in the same list.
func validateUpdateArgs(toDelete []spanconfig.Target, toUpsert []spanconfig.Record) error {
	targetsToUpdate := func(recs []spanconfig.Record) []spanconfig.Target {
		targets := make([]spanconfig.Target, len(recs))
		for i, ent := range recs {
			targets[i] = ent.Target
		}
		return targets
	}(toUpsert)

	for _, list := range [][]spanconfig.Target{toDelete, targetsToUpdate} {
		if err := validateSpanTargets(list); err != nil {
			return err
		}

		targets := make([]spanconfig.Target, len(list))
		copy(targets, list)
		sort.Sort(spanconfig.Targets(targets))
		for i := range targets {
			if targets[i].IsSystemTarget() && targets[i].GetSystemTarget().IsReadOnly() {
				return errors.AssertionFailedf(
					"cannot use read only system target %s as an update argument", targets[i],
				)
			}

			if i == 0 {
				continue
			}

			curTarget := targets[i]
			prevTarget := targets[i-1]
			if curTarget.IsSpanTarget() && prevTarget.IsSpanTarget() {
				if curTarget.GetSpan().Overlaps(prevTarget.GetSpan()) {
					return errors.AssertionFailedf("overlapping spans %s and %s in same list",
						prevTarget.GetSpan(), curTarget.GetSpan())
				}
			}

			if curTarget.IsSystemTarget() && prevTarget.IsSystemTarget() && curTarget.Equal(prevTarget) {
				return errors.AssertionFailedf("duplicate system targets %s in the same list",
					prevTarget.GetSystemTarget())
			}

			// We're dealing with different types of target; no
			// duplication/overlapping is possible.
		}
	}

	return nil
}

// validateSpanTargets returns an error if any spans in the supplied targets
// are invalid or have an empty end key.
func validateSpanTargets(targets []spanconfig.Target) error {
	for _, target := range targets {
		if !target.IsSpanTarget() {
			// Nothing to do.
			continue
		}
		if err := validateSpans(target.GetSpan()); err != nil {
			return err
		}
	}
	return nil
}

// validateSpans returns an error if any of the spans are invalid or have an
// empty end key.
func validateSpans(spans ...roachpb.Span) error {
	for _, span := range spans {
		if !span.Valid() || len(span.EndKey) == 0 {
			return errors.AssertionFailedf("invalid span: %s", span)
		}
	}
	return nil
}

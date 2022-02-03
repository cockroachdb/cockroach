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
	"github.com/cockroachdb/cockroach/pkg/spanconfig/systemspanconfig"
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

// GetSpanConfigEntriesFor is part of the KVAccessor interface.
func (k *KVAccessor) GetSpanConfigEntriesFor(
	ctx context.Context, spans []roachpb.Span,
) (resp []roachpb.SpanConfigEntry, retErr error) {
	entries, err := k.getConfigEntries(ctx, spans)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		spanConfig := entry.Config.GetSpanConfig()
		if spanConfig == nil {
			return nil, errors.AssertionFailedf("did not find span configuration for span %s", entry.Span)
		}
		resp = append(resp, roachpb.SpanConfigEntry{
			Span:   entry.Span,
			Config: *spanConfig,
		})
	}
	return resp, nil
}

// GetSpanConfigEntriesFor is part of the spanconfig.KVAccessor interface.
func (k *KVAccessor) getConfigEntries(
	ctx context.Context, spans []roachpb.Span,
) (resp []roachpb.SystemSpanConfigurationsTableEntry, retErr error) {
	if len(spans) == 0 {
		return resp, nil
	}
	if err := validateSpans(spans); err != nil {
		return nil, err
	}

	getStmt, getQueryArgs := k.constructGetStmtAndArgs(spans)
	it, err := k.ie.QueryIteratorEx(ctx, "get-span-cfgs", k.optionalTxn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getStmt, getQueryArgs...,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			resp, retErr = nil, errors.CombineErrors(retErr, closeErr)
		}
	}()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		span := roachpb.Span{
			Key:    []byte(*row[0].(*tree.DBytes)),
			EndKey: []byte(*row[1].(*tree.DBytes)),
		}
		var conf roachpb.Config
		if err := protoutil.Unmarshal(([]byte)(*row[2].(*tree.DBytes)), &conf); err != nil {
			return nil, err
		}

		resp = append(resp, roachpb.SystemSpanConfigurationsTableEntry{
			Span:   span,
			Config: conf,
		})
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateSpanConfigEntries is part of the KVAccessor interface.
func (k *KVAccessor) UpdateSpanConfigEntries(
	ctx context.Context, toDelete []roachpb.Span, toUpsert []roachpb.SpanConfigEntry,
) error {
	if err := validateUpdateArgs(toDelete, toUpsert); err != nil {
		return err
	}

	tableEntriesToUpsert := make([]roachpb.SystemSpanConfigurationsTableEntry, 0, len(toUpsert))
	for i := range toUpsert {
		tableEntriesToUpsert = append(tableEntriesToUpsert, roachpb.SystemSpanConfigurationsTableEntry{
			Span: toUpsert[i].Span,
			Config: roachpb.Config{
				Union: &roachpb.Config_SpanConfig{
					SpanConfig: &toUpsert[i].Config,
				},
			},
		})
	}
	if k.optionalTxn != nil {
		return k.updateSpanConfigEntriesWithTxn(ctx, toDelete, tableEntriesToUpsert, k.optionalTxn)
	}

	return k.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return k.updateSpanConfigEntriesWithTxn(ctx, toDelete, tableEntriesToUpsert, txn)
	})
}

// GetSystemSpanConfigEntries is part of the spanconfig.KVAccessor interface.
func (k *KVAccessor) GetSystemSpanConfigEntries(
	ctx context.Context,
) (resp []roachpb.SystemSpanConfigEntry, _ error) {
	getSpans := make([]roachpb.Span, 0, 2) // Capacity at most 2

	clusterTarget, err := systemspanconfig.MakeTargetUsingSourceContext(ctx, roachpb.SystemSpanConfigTarget{})
	if err != nil {
		return nil, err
	}
	getSpans = append(getSpans, systemspanconfig.EncodeTarget(clusterTarget))

	tenID, ok := roachpb.TenantFromContext(ctx)
	if !ok || tenID == roachpb.SystemTenantID {
		// We're in the system tenant.
		getSpans = append(getSpans, systemspanconfig.GetHostTenantOnTenantKeyspaceSpan())
	}

	entries, err := k.getConfigEntries(ctx, getSpans)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		target, err := systemspanconfig.DecodeTarget(entry.Span)
		if err != nil {
			return nil, err
		}
		systemSpanConfig := entry.Config.GetSystemSpanConfig()
		if systemSpanConfig == nil {
			return nil, errors.AssertionFailedf(
				"did not find system span configuration for span %s", target,
			)
		}
		t := roachpb.SystemSpanConfigTarget{}
		if target.TargeterTenantID != target.TargeteeTenantID {
			t.TenantID = &target.TargeteeTenantID
		}
		resp = append(resp, roachpb.SystemSpanConfigEntry{
			SystemSpanConfigTarget: t,
			SystemSpanConfig:       *systemSpanConfig,
		})
	}
	return resp, nil
}

// UpdateSystemSpanConfigEntries is part of the spanconfig.KVAccessor interface.
func (k *KVAccessor) UpdateSystemSpanConfigEntries(
	ctx context.Context,
	toDelete []roachpb.SystemSpanConfigTarget,
	toUpsert []roachpb.SystemSpanConfigEntry,
) error {
	if err := validateUpdateSystemSpanConfigArgs(toDelete, toUpsert); err != nil {
		return err
	}
	toDeleteSpans := make([]roachpb.Span, 0, len(toDelete))
	for _, t := range toDelete {
		target, err := systemspanconfig.MakeTargetUsingSourceContext(ctx, t)
		if err != nil {
			return err
		}
		toDeleteSpans = append(
			toDeleteSpans,
			systemspanconfig.EncodeTarget(target),
		)
	}

	toUpsertTableEntries := make([]roachpb.SystemSpanConfigurationsTableEntry, 0, len(toUpsert))
	for i := range toUpsert {
		target, err := systemspanconfig.MakeTargetUsingSourceContext(ctx, toUpsert[i].SystemSpanConfigTarget)
		if err != nil {
			return err
		}
		span := systemspanconfig.EncodeTarget(target)
		toUpsertTableEntries = append(toUpsertTableEntries, roachpb.SystemSpanConfigurationsTableEntry{
			Span: span,
			Config: roachpb.Config{
				Union: &roachpb.Config_SystemSpanConfig{
					SystemSpanConfig: &toUpsert[i].SystemSpanConfig,
				},
			},
		})
	}

	if k.optionalTxn != nil {
		return k.updateSpanConfigEntriesWithTxn(ctx, toDeleteSpans, toUpsertTableEntries, k.optionalTxn)
	}
	return k.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return k.updateSpanConfigEntriesWithTxn(ctx, toDeleteSpans, toUpsertTableEntries, txn)
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

func (k *KVAccessor) updateSpanConfigEntriesWithTxn(
	ctx context.Context,
	toDelete []roachpb.Span,
	toUpsert []roachpb.SystemSpanConfigurationsTableEntry,
	txn *kv.Txn,
) error {
	if txn == nil {
		log.Fatalf(ctx, "expected non-nil txn")
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
		// Nothing left to do.
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
func (k *KVAccessor) constructGetStmtAndArgs(spans []roachpb.Span) (string, []interface{}) {
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
	queryArgs := make([]interface{}, len(spans)*2)
	for i, sp := range spans {
		if i > 0 {
			getStmtBuilder.WriteString(`UNION ALL`)
		}

		startKeyIdx, endKeyIdx := i*2, (i*2)+1
		queryArgs[startKeyIdx] = sp.Key
		queryArgs[endKeyIdx] = sp.EndKey

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
func (k *KVAccessor) constructDeleteStmtAndArgs(toDelete []roachpb.Span) (string, []interface{}) {
	// We're constructing a single delete statement to delete all requested
	// spans. It's of the form:
	//
	//   DELETE FROM span_configurations WHERE (start_key, end_key) IN
	//   (VALUES ( ... 1st span ... ), ( ... 2nd span ...), ... );
	//
	values := make([]string, len(toDelete))
	deleteQueryArgs := make([]interface{}, len(toDelete)*2)
	for i, sp := range toDelete {
		startKeyIdx, endKeyIdx := i*2, (i*2)+1
		deleteQueryArgs[startKeyIdx] = sp.Key
		deleteQueryArgs[endKeyIdx] = sp.EndKey
		values[i] = fmt.Sprintf("($%d::BYTES, $%d::BYTES)",
			startKeyIdx+1, endKeyIdx+1) // prepared statement placeholders (1-indexed)
	}
	deleteStmt := fmt.Sprintf(`DELETE FROM %[1]s WHERE (start_key, end_key) IN (VALUES %[2]s)`,
		k.configurationsTableFQN, strings.Join(values, ", "))
	return deleteStmt, deleteQueryArgs
}

// constructUpsertStmtAndArgs constructs the statement and query arguments
// needed to upsert the given span config entries.
func (k *KVAccessor) constructUpsertStmtAndArgs(
	toUpsert []roachpb.SystemSpanConfigurationsTableEntry,
) (string, []interface{}, error) {
	// We're constructing a single upsert statement to upsert all requested
	// spans. It's of the form:
	//
	//   UPSERT INTO span_configurations (start_key, end_key, config)
	//   VALUES ( ... 1st span ... ), ( ... 2nd span ...), ... ;
	//
	upsertValues := make([]string, len(toUpsert))
	upsertQueryArgs := make([]interface{}, len(toUpsert)*3)
	for i, entry := range toUpsert {
		marshaled, err := protoutil.Marshal(&entry.Config)
		if err != nil {
			return "", nil, err
		}

		startKeyIdx, endKeyIdx, configIdx := i*3, (i*3)+1, (i*3)+2
		upsertQueryArgs[startKeyIdx] = entry.Span.Key
		upsertQueryArgs[endKeyIdx] = entry.Span.EndKey
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
	toUpsert []roachpb.SystemSpanConfigurationsTableEntry,
) (string, []interface{}) {
	// We want to validate that upserting spans does not break the invariant
	// that spans in the table are non-overlapping. We only need to validate
	// the spans that are being upserted, and can use a query similar to
	// what we do in GetSpanConfigEntriesFor. For a single upserted span, we
	// want effectively validate using:
	//
	//   SELECT count(*) = 1 FROM system.span_configurations
	//    WHERE start_key < $end AND end_key > $start
	//
	// Applying the GetSpanConfigEntriesFor treatment, we can arrive at:
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
		validationQueryArgs[startKeyIdx] = entry.Span.Key
		validationQueryArgs[endKeyIdx] = entry.Span.EndKey

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

// validateUpdateArgs returns an error the arguments to UpdateSpanConfigEntries
// are malformed. All spans included in the toDelete and toUpsert list are
// expected to be valid and to have non-empty end keys. Spans are also expected
// to be non-overlapping with other spans in the same list.
func validateUpdateArgs(toDelete []roachpb.Span, toUpsert []roachpb.SpanConfigEntry) error {
	spansToUpdate := func(ents []roachpb.SpanConfigEntry) []roachpb.Span {
		spans := make([]roachpb.Span, len(ents))
		for i, ent := range ents {
			spans[i] = ent.Span
		}
		return spans
	}(toUpsert)

	for _, list := range [][]roachpb.Span{toDelete, spansToUpdate} {
		if err := validateSpans(list); err != nil {
			return err
		}

		spans := make([]roachpb.Span, len(list))
		copy(spans, list)
		sort.Sort(roachpb.Spans(spans))
		for i := range spans {
			if i == 0 {
				continue
			}

			if spans[i].Overlaps(spans[i-1]) {
				return errors.AssertionFailedf("overlapping spans %s and %s in same list",
					spans[i-1], spans[i])
			}
		}
	}

	return nil
}

// validateSpans returns an error if any of the spans are invalid or have an
// empty end key.
func validateSpans(spans []roachpb.Span) error {
	for _, span := range spans {
		if !span.Valid() || len(span.EndKey) == 0 {
			return errors.AssertionFailedf("invalid span: %s", span)
		}
	}
	return nil
}

// validateUpdateSystemSpanConfigArgs validates arguments to a call to
// UpdateSystemSpanConfigs by ensuring there are no duplicate targets, either in
// one of the lists or across lists.
func validateUpdateSystemSpanConfigArgs(
	toDelete []roachpb.SystemSpanConfigTarget, toUpsert []roachpb.SystemSpanConfigEntry,
) error {
	foundClusterTarget := false
	targets := make(map[roachpb.TenantID]struct{})

	validateTarget := func(target roachpb.SystemSpanConfigTarget) error {
		tenID := target.TenantID
		if tenID == nil {
			if foundClusterTarget {
				return errors.AssertionFailedf("duplicate cluster target found")
			}
			foundClusterTarget = true
			return nil
		}
		if _, found := targets[*tenID]; found {
			return errors.AssertionFailedf("duplicate target found %s", target)
		}
		targets[*tenID] = struct{}{}
		return nil
	}

	for _, target := range toDelete {
		if err := validateTarget(target); err != nil {
			return err
		}
	}
	for _, entry := range toUpsert {
		if err := validateTarget(entry.SystemSpanConfigTarget); err != nil {
			return err
		}
	}
	return nil
}

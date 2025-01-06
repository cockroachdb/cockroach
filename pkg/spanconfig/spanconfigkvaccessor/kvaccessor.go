// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvaccessor

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// batchSizeSetting is a hidden cluster setting to control how many span config
// records we access in a single batch, beyond which we start paginating.
// No limit enforced if set to zero (or something negative).
var batchSizeSetting = settings.RegisterIntSetting(
	settings.SystemOnly,
	"spanconfig.kvaccessor.batch_size",
	`number of span config records to access in a single batch`,
	10000,
)

// KVAccessor provides read/write access to all the span configurations for a
// CRDB cluster. It's a concrete implementation of the KVAccessor interface.
type KVAccessor struct {
	db *kv.DB
	ie isql.Executor
	// optionalTxn captures the transaction we're scoped to; it's allowed to be
	// nil. If nil, it's unsafe to use multiple times as part of the same
	// request with any expectation of transactionality -- we're responsible for
	// opening a fresh txn.
	optionalTxn *kv.Txn
	settings    *cluster.Settings
	clock       *hlc.Clock

	// configurationsTableFQN is typically 'system.public.span_configurations',
	// but left configurable for ease-of-testing.
	configurationsTableFQN string

	knobs *spanconfig.TestingKnobs
}

var _ spanconfig.KVAccessor = &KVAccessor{}

// New constructs a new KVAccessor.
func New(
	db *kv.DB,
	ie isql.Executor,
	settings *cluster.Settings,
	clock *hlc.Clock,
	configurationsTableFQN string,
	knobs *spanconfig.TestingKnobs,
) *KVAccessor {
	if _, err := parser.ParseQualifiedTableName(configurationsTableFQN); err != nil {
		panic(fmt.Sprintf("unabled to parse configurations table FQN: %s", configurationsTableFQN))
	}

	return newKVAccessor(db, ie, settings, clock, configurationsTableFQN, knobs, nil /* optionalTxn */)
}

// WithTxn is part of the KVAccessor interface.
func (k *KVAccessor) WithTxn(ctx context.Context, txn *kv.Txn) spanconfig.KVAccessor {
	if k.optionalTxn != nil {
		log.Fatalf(ctx, "KVAccessor already scoped to txn (was .WithTxn(...) chained multiple times?)")
	}
	return newKVAccessor(k.db, k.ie, k.settings, k.clock, k.configurationsTableFQN, k.knobs, txn)
}

// WithISQLTxn is part of the KVAccessor interface.
func (k *KVAccessor) WithISQLTxn(ctx context.Context, txn isql.Txn) spanconfig.KVAccessor {
	if k.optionalTxn != nil {
		log.Fatalf(ctx, "KVAccessor already scoped to txn (was .WithISQLTxn(...) chained multiple times?)")
	}
	return newKVAccessor(txn.KV().DB(), txn, k.settings, k.clock, k.configurationsTableFQN, k.knobs, txn.KV())
}

// GetAllSystemSpanConfigsThatApply is part of the spanconfig.KVAccessor
// interface.
func (k *KVAccessor) GetAllSystemSpanConfigsThatApply(
	ctx context.Context, id roachpb.TenantID,
) (spanConfigs []roachpb.SpanConfig, _ error) {
	hostSetOnTenant, err := spanconfig.MakeTenantKeyspaceTarget(
		roachpb.SystemTenantID, id,
	)
	if err != nil {
		return nil, err
	}

	// Construct a list of system targets whose corresponding system span configs
	// apply to ranges of the given tenant ID. These are:
	// 1. The system span config that applies over the entire keyspace. (set by
	// the host).
	// 2. The system span config set by the host over just the tenant's keyspace.
	// 3. The system span config set by the tenant over its own keyspace.
	targets := []spanconfig.Target{
		spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
		spanconfig.MakeTargetFromSystemTarget(hostSetOnTenant),
	}

	// We only need to do this for secondary tenants; we've already added this
	// target if tenID == system tenant.
	if id != roachpb.SystemTenantID {
		target, err := spanconfig.MakeTenantKeyspaceTarget(id, id)
		if err != nil {
			return nil, err
		}
		targets = append(targets, spanconfig.MakeTargetFromSystemTarget(target))
	}

	records, err := k.GetSpanConfigRecords(ctx, targets)
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		spanConfigs = append(spanConfigs, record.GetConfig())
	}

	return spanConfigs, nil
}

// GetSpanConfigRecords is part of the KVAccessor interface.
func (k *KVAccessor) GetSpanConfigRecords(
	ctx context.Context, targets []spanconfig.Target,
) ([]spanconfig.Record, error) {
	if k.optionalTxn != nil {
		return k.getSpanConfigRecordsWithTxn(ctx, targets, k.optionalTxn)
	}

	var records []spanconfig.Record
	if err := k.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		records, err = k.getSpanConfigRecordsWithTxn(ctx, targets, txn)
		return err
	}); err != nil {
		return nil, err
	}

	return records, nil
}

// UpdateSpanConfigRecords is part of the KVAccessor interface.
func (k *KVAccessor) UpdateSpanConfigRecords(
	ctx context.Context,
	toDelete []spanconfig.Target,
	toUpsert []spanconfig.Record,
	minCommitTS, maxCommitTS hlc.Timestamp,
) error {
	log.VInfof(ctx, 2, "kv accessor updating span configs: toDelete=%+v, toUpsert=%+v, minCommitTS=%s, maxCommitTS=%s", toDelete, toUpsert, minCommitTS, maxCommitTS)

	if k.optionalTxn != nil {
		return k.updateSpanConfigRecordsWithTxn(ctx, toDelete, toUpsert, k.optionalTxn, minCommitTS, maxCommitTS)
	}

	if fn := k.knobs.KVAccessorPreCommitMinTSWaitInterceptor; fn != nil {
		fn()
	}
	if err := k.clock.SleepUntil(ctx, minCommitTS); err != nil {
		return errors.Wrapf(err, "waiting for clock to be in advance of minimum commit timestamp (%s)",
			minCommitTS)
	}
	// Given that our clock reading is past the supplied minCommitTS now,
	// we can go ahead and create a transaction and proceed to perform the
	// update.
	return k.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return k.updateSpanConfigRecordsWithTxn(ctx, toDelete, toUpsert, txn, minCommitTS, maxCommitTS)
	})
}

func newKVAccessor(
	db *kv.DB,
	ie isql.Executor,
	settings *cluster.Settings,
	clock *hlc.Clock,
	configurationsTableFQN string,
	knobs *spanconfig.TestingKnobs,
	optionalTxn *kv.Txn,
) *KVAccessor {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}

	return &KVAccessor{
		db:                     db,
		ie:                     ie,
		clock:                  clock,
		optionalTxn:            optionalTxn,
		settings:               settings,
		configurationsTableFQN: configurationsTableFQN,
		knobs:                  knobs,
	}
}

func (k *KVAccessor) getSpanConfigRecordsWithTxn(
	ctx context.Context, targets []spanconfig.Target, txn *kv.Txn,
) ([]spanconfig.Record, error) {
	if txn == nil {
		log.Fatalf(ctx, "expected non-nil txn")
	}

	if len(targets) == 0 {
		return nil, nil
	}
	if err := validateSpanTargets(targets); err != nil {
		return nil, err
	}

	var records []spanconfig.Record
	if err := k.paginate(len(targets), func(startIdx, endIdx int) (retErr error) {
		targetsBatch := targets[startIdx:endIdx]
		getStmt, getQueryArgs := k.constructGetStmtAndArgs(targetsBatch)
		it, err := k.ie.QueryIteratorEx(ctx, "get-span-cfgs", txn,
			sessiondata.NodeUserSessionDataOverride,
			getStmt, getQueryArgs...,
		)
		if err != nil {
			return err
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
				return err
			}

			record, err := spanconfig.MakeRecord(spanconfig.DecodeTarget(span), conf)
			if err != nil {
				return err
			}
			records = append(records, record)
		}
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return records, nil
}

func (k *KVAccessor) updateSpanConfigRecordsWithTxn(
	ctx context.Context,
	toDelete []spanconfig.Target,
	toUpsert []spanconfig.Record,
	txn *kv.Txn,
	minCommitTS, maxCommitTS hlc.Timestamp,
) error {
	if txn == nil {
		log.Fatalf(ctx, "expected non-nil txn")
	}

	if !minCommitTS.Less(maxCommitTS) {
		return errors.AssertionFailedf("invalid commit interval [%s, %s)", minCommitTS, maxCommitTS)
	}

	if txn.ReadTimestamp().Less(minCommitTS) {
		return errors.AssertionFailedf(
			"transaction's read timestamp (%s) below the minimum commit timestamp (%s)",
			txn.ReadTimestamp(), minCommitTS,
		)
	}

	if maxCommitTS.Less(txn.ReadTimestamp()) {
		return spanconfig.NewCommitTimestampOutOfBoundsError()
	}

	// Set the deadline of the transaction so that it commits before the maximum
	// commit timestamp.
	if err := txn.UpdateDeadline(ctx, maxCommitTS); err != nil {
		return errors.Wrapf(err, "transaction deadline could not be updated")
	}

	if fn := k.knobs.KVAccessorPostCommitDeadlineSetInterceptor; fn != nil {
		fn(txn)
	}

	if err := validateUpdateArgs(toDelete, toUpsert); err != nil {
		return err
	}

	if len(toDelete) > 0 {
		if err := k.paginate(len(toDelete), func(startIdx, endIdx int) error {
			toDeleteBatch := toDelete[startIdx:endIdx]
			deleteStmt, deleteQueryArgs := k.constructDeleteStmtAndArgs(toDeleteBatch)
			n, err := k.ie.ExecEx(ctx, "delete-span-cfgs", txn,
				sessiondata.NodeUserSessionDataOverride,
				deleteStmt, deleteQueryArgs...,
			)
			if err != nil {
				return err
			}
			if n != len(toDeleteBatch) {
				return errors.AssertionFailedf("expected to delete %d row(s), deleted %d", len(toDeleteBatch), n)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if len(toUpsert) == 0 {
		return nil // nothing left to do
	}

	return k.paginate(len(toUpsert), func(startIdx, endIdx int) error {
		toUpsertBatch := toUpsert[startIdx:endIdx]
		upsertStmt, upsertQueryArgs, err := k.constructUpsertStmtAndArgs(toUpsertBatch)
		if err != nil {
			return err
		}
		if n, err := k.ie.ExecEx(ctx, "upsert-span-cfgs", txn,
			sessiondata.NodeUserSessionDataOverride,
			upsertStmt, upsertQueryArgs...,
		); err != nil {
			return err
		} else if n != len(toUpsertBatch) {
			return errors.AssertionFailedf("expected to upsert %d row(s), upserted %d", len(toUpsertBatch), n)
		}

		validationStmt, validationQueryArgs := k.constructValidationStmtAndArgs(toUpsertBatch)
		if datums, err := k.ie.QueryRowEx(ctx, "validate-span-cfgs", txn,
			sessiondata.NodeUserSessionDataOverride,
			validationStmt, validationQueryArgs...,
		); err != nil {
			return err
		} else if valid := bool(tree.MustBeDBool(datums[0])); !valid {
			return errors.AssertionFailedf("expected to find single row containing upserted spans")
		}
		return nil
	})
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
		cfg := record.GetConfig()
		marshaled, err := protoutil.Marshal(&cfg)
		if err != nil {
			return "", nil, err
		}

		startKeyIdx, endKeyIdx, configIdx := i*3, (i*3)+1, (i*3)+2
		upsertQueryArgs[startKeyIdx] = record.GetTarget().Encode().Key
		upsertQueryArgs[endKeyIdx] = record.GetTarget().Encode().EndKey
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
	//   -- verify only a single span overlaps with [$start, $end)
	//   SELECT count(*) = 1 FROM system.span_configurations
	//    WHERE start_key < $end AND end_key > $start
	//
	// With the naive form above that translates to an unbounded index scan on
	// followed by a filter. Since start_key < end_key, and that spans are
	// non-overlapping, we can instead do the following:
	//
	//   SELECT bool_and(valid) FROM (
	//    SELECT bool_and(prev_end_key IS NULL OR start_key >= prev_end_key) AS valid FROM (
	//     SELECT start_key, lag(end_key, 1) OVER (ORDER BY start_key) AS prev_end_key FROM span_configurations
	//     WHERE start_key >= $start AND start_key < $end
	//    )
	//    UNION ALL
	//    SELECT * FROM (
	//     SELECT $start >= end_key FROM span_configurations
	//     WHERE start_key < $start ORDER BY start_key DESC LIMIT 1
	//    )
	//   )
	//
	// The idea is to first find all spans that start within the span being
	// upserted[1], compare each start key to the preceding end key[2] (if any)
	// and ensure that they're non-overlapping[3]. We also verify the span with
	// the start key immediately preceding the span being upserted[4]; ensuring
	// that our upserted span does not overlap with it[5].
	//
	// When multiple spans are being upserted, we validate instead the span
	// straddling all the individual spans being upserted, i.e.
	// [$smallest-start-key, $largest-end-key).
	//
	// [1]: WHERE start_key >= $start AND start_key < $end
	// [2]: lag(end_key, 1) OVER (ORDER BY start_key) AS prev_end_key
	// [3]: start_key >= prev_end_key
	// [4]: WHERE start_key < $start ORDER BY start_key DESC LIMIT 1
	// [5]: $start >= end_key
	//
	targetsToUpsert := spanconfig.TargetsFromRecords(toUpsert)
	sort.Sort(spanconfig.Targets(targetsToUpsert))

	validationQueryArgs := make([]interface{}, 2)
	validationQueryArgs[0] = targetsToUpsert[0].Encode().Key
	// NB: This is the largest key due to sort above + validation at the caller
	// than ensures non-overlapping upsert spans.
	validationQueryArgs[1] = targetsToUpsert[len(targetsToUpsert)-1].Encode().EndKey

	validationStmt := fmt.Sprintf(`
SELECT bool_and(valid) FROM (
  SELECT bool_and(prev_end_key IS NULL OR start_key >= prev_end_key) AS valid FROM (
    SELECT start_key, lag(end_key, 1) OVER (ORDER BY start_key) AS prev_end_key FROM %[1]s
    WHERE start_key >= $1 AND start_key < $2
  )
  UNION ALL
  SELECT * FROM (
    SELECT $1 >= end_key FROM %[1]s
    WHERE start_key < $1 ORDER BY start_key DESC LIMIT 1
  )
)`, k.configurationsTableFQN)

	return validationStmt, validationQueryArgs
}

// validateUpdateArgs returns an error the arguments to UpdateSpanConfigRecords
// are malformed. All spans included in the toDelete and toUpsert list are
// expected to be valid and to have non-empty end keys. Spans are also expected
// to be non-overlapping with other spans in the same list.
func validateUpdateArgs(toDelete []spanconfig.Target, toUpsert []spanconfig.Record) error {
	targetsToUpdate := spanconfig.TargetsFromRecords(toUpsert)
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

// paginate is a helper method to paginate through a list with the batch size
// controlled by the spanconfig.kvaccessor.batch_size setting. It invokes the
// provided callback with the [start,end) indexes over the original list.
func (k *KVAccessor) paginate(totalLen int, f func(startIdx, endIdx int) error) error {
	batchSize := math.MaxInt32
	if b := batchSizeSetting.Get(&k.settings.SV); int(b) > 0 {
		batchSize = int(b) // check for overflow, negative or 0 value
	}

	if fn := k.knobs.KVAccessorBatchSizeOverrideFn; fn != nil {
		batchSize = fn()
	}

	for i := 0; i < totalLen; i += batchSize {
		j := i + batchSize
		if j > totalLen {
			j = totalLen
		}

		if fn := k.knobs.KVAccessorPaginationInterceptor; fn != nil {
			fn()
		}

		if err := f(i, j); err != nil {
			return err
		}
	}
	return nil
}

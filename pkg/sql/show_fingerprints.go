// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionprotectedts"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type showFingerprintsNode struct {
	zeroInputPlanNode
	columns colinfo.ResultColumns

	tableDesc catalog.TableDescriptor
	indexes   []catalog.Index

	tenantSpec tenantSpec
	options    *resolvedShowTenantFingerprintOptions

	run showFingerprintsRun
}

// ShowFingerprints statement fingerprints the data in each index of a table.
// For each index, a full index scan is run to hash every row with the fnv64
// hash. For the primary index, all table columns are included in the hash,
// whereas for secondary indexes, the index cols + the primary index cols + the
// STORING cols are included. The hashed rows are all combined with XOR using
// distsql.
//
// Our hash functions expect input of type BYTES (or string but we use bytes
// here), so we have to convert any datums that are not BYTES. This is currently
// done by round tripping through the string representation of the column
// (`::string::bytes`) and is an obvious area for improvement in the next
// version.
//
// To extract the fingerprints at some point in the past, the following
// query can be used:
//
//	SELECT * FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE foo] AS OF SYSTEM TIME xxx
func (p *planner) ShowFingerprints(
	ctx context.Context, n *tree.ShowFingerprints,
) (planNode, error) {

	op := "SHOW EXPERIMENTAL_FINGERPRINTS"
	evalOptions, err := evalShowFingerprintOptions(ctx, n.Options, p.EvalContext(), p.SemaCtx(),
		op, p.ExprEvaluator(op))
	if err != nil {
		return nil, err
	}

	if n.TenantSpec != nil {
		// Tenant fingerprints use the KV fingerprint method and can't exclude columns this way
		if evalOptions.excludedUserColumns != nil {
			err = pgerror.New(pgcode.InvalidParameterValue, "cannot use the EXCLUDE COLUMNS option when fingerprinting a tenant.")
			return nil, err
		}
		return p.planShowTenantFingerprint(ctx, n.TenantSpec, evalOptions)
	}

	// Only allow this for virtual clusters as it uses the KV fingerprint method instead of SQL
	if !evalOptions.startTimestamp.IsEmpty() {
		err = pgerror.New(pgcode.InvalidParameterValue, "cannot use the START TIMESTAMP option when fingerprinting a table.")
		return nil, err
	}

	// We avoid the cache so that we can observe the fingerprints without
	// taking a lease, like other SHOW commands.
	tableDesc, err := p.ResolveUncachedTableDescriptorEx(
		ctx, n.Table, true /*required*/, tree.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	return &showFingerprintsNode{
		columns:   colinfo.ShowFingerprintsColumns,
		tableDesc: tableDesc,
		indexes:   tableDesc.ActiveIndexes(),
		options:   evalOptions,
	}, nil
}

type resolvedShowTenantFingerprintOptions struct {
	startTimestamp      hlc.Timestamp
	excludedUserColumns []string
}

func evalShowFingerprintOptions(
	ctx context.Context,
	options tree.ShowFingerprintOptions,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	op string,
	eval exprutil.Evaluator,
) (*resolvedShowTenantFingerprintOptions, error) {
	r := &resolvedShowTenantFingerprintOptions{}
	if options.StartTimestamp != nil {
		ts, err := asof.EvalSystemTimeExpr(ctx, evalCtx, semaCtx, options.StartTimestamp, op, asof.ShowTenantFingerprint)
		if err != nil {
			return nil, err
		}
		r.startTimestamp = ts
	}

	if options.ExcludedUserColumns != nil {
		cols, err := eval.StringArray(
			ctx, tree.Exprs(options.ExcludedUserColumns))

		if err != nil {
			return nil, err
		}
		r.excludedUserColumns = cols
	}

	return r, nil
}

func (p *planner) planShowTenantFingerprint(
	ctx context.Context, ts *tree.TenantSpec, evalOptions *resolvedShowTenantFingerprintOptions,
) (planNode, error) {
	if err := CanManageTenant(ctx, p); err != nil {
		return nil, err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "fingerprint", p.execCfg.Settings); err != nil {
		return nil, err
	}

	tspec, err := p.planTenantSpec(ctx, ts, "SHOW EXPERIMENTAL_FINGERPRINTS FROM VIRTUAL CLUSTER")
	if err != nil {
		return nil, err
	}

	return &showFingerprintsNode{
		columns:    colinfo.ShowTenantFingerprintsColumns,
		tenantSpec: tspec,
		options:    evalOptions,
	}, nil
}

// showFingerprintsRun contains the run-time state of
// showFingerprintsNode during local execution.
type showFingerprintsRun struct {
	rowIdx int
	// values stores the current row, updated by Next().
	values []tree.Datum
}

func (n *showFingerprintsNode) startExec(_ runParams) error {
	if n.tenantSpec != nil {
		n.run.values = []tree.Datum{tree.DNull, tree.DNull, tree.DNull, tree.DNull}
		return nil
	}

	n.run.values = []tree.Datum{tree.DNull, tree.DNull}
	return nil
}

// protectTenantSpanWithSession creates a protected timestamp record
// for the given tenant ID at the read timestamp of the current
// transaction. The PTS record will be tied to the given sessionID.
//
// The caller should call the returned cleanup function to release the
// PTS record.
func protectTenantSpanWithSession(
	ctx context.Context,
	execCfg *ExecutorConfig,
	tenantID roachpb.TenantID,
	sessionID clusterunique.ID,
	tsToProtect hlc.Timestamp,
) (func(), error) {
	ptsRecordID := uuid.MakeV4()
	ptsRecord := sessionprotectedts.MakeRecord(
		ptsRecordID,
		// TODO(ssd): The type here seems weird. I think this
		// is correct in that we use this to compare against
		// the session_id table which returns the stringified
		// session ID. But, maybe we can make this clearer.
		[]byte(sessionID.String()),
		tsToProtect,
		ptpb.MakeTenantsTarget([]roachpb.TenantID{tenantID}),
	)
	log.Infof(ctx, "protecting timestamp: %#+v", ptsRecord)
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
		return pts.Protect(ctx, ptsRecord)
	}); err != nil {
		return nil, err
	}

	releasePTS := func() {
		if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
			return pts.Release(ctx, ptsRecordID)
		}); err != nil {
			log.Warningf(ctx, "failed to release protected timestamp %s: %v", ptsRecordID, err)
		}
	}
	return releasePTS, nil
}

func (n *showFingerprintsNode) nextTenant(params runParams) (bool, error) {
	if n.run.rowIdx > 0 {
		return false, nil
	}

	tinfo, err := n.tenantSpec.getTenantInfo(params.ctx, params.p)
	if err != nil {
		return false, err
	}

	tid, err := roachpb.MakeTenantID(tinfo.ID)
	if err != nil {
		return false, err
	}

	// We want to write a protected timestamp record at the earliest timestamp
	// that the fingerprint query is going to read from. When fingerprinting
	// revisions, this will be the specified start time.
	tsToProtect := params.p.EvalContext().Txn.ReadTimestamp()
	if n.options != nil && !n.options.startTimestamp.IsEmpty() {
		if !n.options.startTimestamp.LessEq(tsToProtect) {
			return false, pgerror.Newf(pgcode.InvalidParameterValue, `start timestamp %s is greater than the end timestamp %s`,
				n.options.startTimestamp.String(), tsToProtect.String())
		}
		tsToProtect = n.options.startTimestamp
	}
	cleanup, err := protectTenantSpanWithSession(
		params.ctx,
		params.p.ExecCfg(),
		tid,
		params.p.ExtendedEvalContext().SessionID,
		tsToProtect,
	)
	if err != nil {
		return false, err
	}
	defer cleanup()

	var startTime hlc.Timestamp
	var allRevisions bool
	if n.options != nil && !n.options.startTimestamp.IsEmpty() {
		startTime = n.options.startTimestamp
		allRevisions = true
	}

	// TODO(dt): remove conditional if we make MakeTenantSpan do this.
	span := keys.MakeTenantSpan(tid)
	if tid.IsSystem() {
		span = roachpb.Span{Key: keys.TableDataMin, EndKey: keys.TableDataMax}
	}

	fingerprint, err := params.p.FingerprintSpan(params.ctx,
		span,
		startTime,
		allRevisions,
		false /* stripped */)
	if err != nil {
		return false, err
	}

	endTime := hlc.Timestamp{
		WallTime: params.p.EvalContext().GetTxnTimestamp(time.Microsecond).UnixNano(),
	}
	n.run.values[0] = tree.NewDString(string(tinfo.Name))
	if !startTime.IsEmpty() {
		n.run.values[1] = eval.TimestampToDecimalDatum(startTime)
	}
	n.run.values[2] = eval.TimestampToDecimalDatum(endTime)
	n.run.values[3] = tree.NewDInt(tree.DInt(fingerprint))
	n.run.rowIdx++

	return true, nil
}

func (n *showFingerprintsNode) Next(params runParams) (bool, error) {
	if n.tenantSpec != nil {
		return n.nextTenant(params)
	}

	if n.run.rowIdx >= len(n.indexes) {
		return false, nil
	}
	index := n.indexes[n.run.rowIdx]
	excludedColumns := []string{}
	if n.options != nil && len(n.options.excludedUserColumns) > 0 {
		excludedColumns = append(excludedColumns, n.options.excludedUserColumns...)
	}
	sql, err := BuildFingerprintQueryForIndex(n.tableDesc, index, excludedColumns)
	if err != nil {
		return false, err
	}
	// If we're in an AOST context, propagate it to the inner statement so that
	// the inner statement gets planned with planner.avoidLeasedDescriptors set,
	// like the outer one.
	if params.p.EvalContext().AsOfSystemTime != nil {
		ts := params.p.txn.ReadTimestamp()
		sql = sql + " AS OF SYSTEM TIME " + ts.AsOfSystemTime()
	}

	fingerprintCols, err := params.p.InternalSQLTxn().QueryRowEx(
		params.ctx, "hash-fingerprint",
		params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		sql,
	)
	if err != nil {
		return false, err
	}

	if len(fingerprintCols) != 1 {
		return false, errors.AssertionFailedf(
			"unexpected number of columns returned: 1 vs %d",
			len(fingerprintCols))
	}
	fingerprint := fingerprintCols[0]

	n.run.values[0] = tree.NewDString(index.GetName())
	n.run.values[1] = fingerprint
	n.run.rowIdx++
	return true, nil
}

func BuildFingerprintQueryForIndex(
	tableDesc catalog.TableDescriptor, index catalog.Index, ignoredColumns []string,
) (string, error) {
	cols := make([]string, 0, len(tableDesc.PublicColumns()))
	var numBytesCols int
	addColumn := func(col catalog.Column) {
		if slices.Contains(ignoredColumns, col.GetName()) {
			return
		}

		var colNameOrExpr string
		if col.IsExpressionIndexColumn() {
			colNameOrExpr = fmt.Sprintf("(%s)", col.GetComputeExpr())
		} else {
			name := col.GetName()
			colNameOrExpr = tree.NameStringP(&name)
		}
		// TODO(dan): This is known to be a flawed way to fingerprint. Any datum
		// with the same string representation is fingerprinted the same, even
		// if they're different types.
		switch col.GetType().Family() {
		case types.BytesFamily:
			cols = append(cols, fmt.Sprintf("%s:::bytes", colNameOrExpr))
			numBytesCols++
		case types.StringFamily:
			cols = append(cols, fmt.Sprintf("%s:::string", colNameOrExpr))
		default:
			cols = append(cols, fmt.Sprintf("%s::string", colNameOrExpr))
		}
	}

	if index.Primary() {
		for _, col := range tableDesc.PublicColumns() {
			addColumn(col)
		}
	} else {
		for i := 0; i < index.NumKeyColumns(); i++ {
			col, err := catalog.MustFindColumnByID(tableDesc, index.GetKeyColumnID(i))
			if err != nil {
				return "", err
			}
			addColumn(col)
		}
		for i := 0; i < index.NumKeySuffixColumns(); i++ {
			col, err := catalog.MustFindColumnByID(tableDesc, index.GetKeySuffixColumnID(i))
			if err != nil {
				return "", err
			}
			addColumn(col)
		}
		for i := 0; i < index.NumSecondaryStoredColumns(); i++ {
			col, err := catalog.MustFindColumnByID(tableDesc, index.GetStoredColumnID(i))
			if err != nil {
				return "", err
			}
			addColumn(col)
		}
	}

	if len(cols) != numBytesCols && numBytesCols != 0 {
		// Currently, cols has a mix of BYTES and STRING types, but fnv64
		// requires all arguments to be of the same type. We'll cast less
		// frequent type to the other.
		from, to := "::bytes", "::string"
		if numBytesCols > len(cols)/2 {
			// BYTES is more frequent.
			from, to = "::string", "::bytes"
		}
		for i := range cols {
			if strings.HasSuffix(cols[i], from) {
				cols[i] = cols[i] + to
			}
		}
	}

	// The fnv64 hash was chosen mostly due to speed. I did an AS OF SYSTEM TIME
	// fingerprint over 31GiB on a 4 node production cluster (with no other
	// traffic to try and keep things comparable). The cluster was restarted in
	// between each run. Resulting times:
	//
	//  fnv => 17m
	//  sha512 => 1h6m
	//  sha265 => 1h6m
	//  fnv64 (again) => 17m
	//
	// TODO(dan): If/when this ever loses its EXPERIMENTAL prefix and gets
	// exposed to users, consider adding a version to the fingerprint output.
	sql := fmt.Sprintf(`SELECT
	  xor_agg(fnv64(%s))::string AS fingerprint
	  FROM [%d AS t]@{FORCE_INDEX=[%d]}
	`, strings.Join(cols, `,`), tableDesc.GetID(), index.GetID())
	if index.IsPartial() {
		sql = fmt.Sprintf("%s WHERE %s", sql, index.GetPredicate())
	}
	return sql, nil
}

func (n *showFingerprintsNode) Values() tree.Datums     { return n.run.values }
func (n *showFingerprintsNode) Close(_ context.Context) {}

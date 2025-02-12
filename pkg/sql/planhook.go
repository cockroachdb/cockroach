// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// planHookFn is a function that can intercept a statement being planned and
// provide an alternate implementation. It's primarily intended to allow
// implementation of certain sql statements to live outside of the sql package.
//
// To intercept a statement the function should return a non-nil function for
// `fn` as well as the appropriate sqlbase.ResultColumns describing the results
// it will return (if any). `fn` will be called in a goroutine during the
// `Start` phase of plan execution.
type planHookFn func(
	context.Context, tree.Statement, PlanHookState,
) (fn PlanHookRowFn, header colinfo.ResultColumns, avoidBuffering bool, err error)

// PlanHookTypeCheckFn is a function that can intercept a statement being
// prepared and type check its arguments. It exists in parallel to PlanHookFn.
// This exists so that we can ensure that the PlanHookFn is always called in a
// context where placeholders are populated. When opaque statements which are
// associated with a PlanHook are prepared, the resultant plan is not marked
// as executable. Instead, the statement will be re-prepared in the context of
// execution will the same EvalContext and placeholders as will be passed into
// its returned PlanHookRowFn.
//
// This function should detect if the statement matches the expectation for
// the PlanHook and should type-check (but not evaluate) any expressions which
// may use placeholders.
type PlanHookTypeCheckFn func(
	context.Context, tree.Statement, PlanHookState,
) (matched bool, header colinfo.ResultColumns, err error)

// PlanHookRowFn describes the row-production for hook-created plans. The
// channel argument is used to return results to the plan's runner. It's
// a blocking channel, so implementors should be careful to only use blocking
// sends on it when necessary.
//
// TODO(dt): should this take runParams like a normal planNode.Next?
type PlanHookRowFn func(context.Context, chan<- tree.Datums) error

type planHook struct {
	name      string
	fn        planHookFn
	typeCheck PlanHookTypeCheckFn
}

var planHooks []planHook

// PlanHookState exposes the subset of planner needed by plan hooks.
// We pass this as one interface, rather than individually passing each field or
// interface as we find we need them, to avoid churn in the planHookFn sig and
// the hooks that implement it.
//
// The PlanHookState is used by modules that are under the CCL. Since the OSS
// modules cannot depend on the CCL modules, the CCL modules need to inform the
// planner when they should be invoked (via plan hooks). The only way for the
// CCL statements to get access to a "planner" is through this PlanHookState
// that gets passed back due to this inversion of roles.
type PlanHookState interface {
	resolver.SchemaResolver
	SemaCtx() *tree.SemaContext
	ExtendedEvalContext() *extendedEvalContext
	SessionData() *sessiondata.SessionData
	SessionDataMutatorIterator() *sessionDataMutatorIterator
	ExecCfg() *ExecutorConfig
	DistSQLPlanner() *DistSQLPlanner
	LeaseMgr() *lease.Manager
	ExprEvaluator(op string) exprutil.Evaluator
	User() username.SQLUsername
	AuthorizationAccessor
	// The role create/drop call into OSS code to reuse plan nodes.
	// TODO(mberhault): it would be easier to just pass a planner to plan hooks.
	GetAllRoles(ctx context.Context) (map[username.SQLUsername]bool, error)
	BumpRoleMembershipTableVersion(ctx context.Context) error
	EvalAsOfTimestamp(
		ctx context.Context,
		asOf tree.AsOfClause,
		opts ...asof.EvalOption,
	) (eval.AsOfSystemTime, error)
	ResolveMutableTableDescriptor(ctx context.Context, tn *tree.TableName, required bool, requiredType tree.RequiredTableKind) (prefix catalog.ResolvedObjectPrefix, table *tabledesc.Mutable, err error)
	ShowCreate(
		ctx context.Context, dbPrefix string, allHydratedDescs []catalog.Descriptor, desc catalog.TableDescriptor, displayOptions ShowCreateDisplayOptions,
	) (string, error)
	MigrationJobDeps() upgrade.JobDeps
	SpanConfigReconciler() spanconfig.Reconciler
	SpanStatsConsumer() keyvisualizer.SpanStatsConsumer
	BufferClientNotice(ctx context.Context, notice pgnotice.Notice)
	SendClientNotice(ctx context.Context, notice pgnotice.Notice, immediateFlush bool) error
	Txn() *kv.Txn
	LookupTenantInfo(ctx context.Context, tenantSpec *tree.TenantSpec, op string) (*mtinfopb.TenantInfo, error)
	GetAvailableTenantID(ctx context.Context, name roachpb.TenantName) (roachpb.TenantID, error)
	InternalSQLTxn() descs.Txn
}

var _ jobsauth.AuthorizationAccessor = PlanHookState(nil)

// AddPlanHook adds a hook used to short-circuit creating a planNode from a
// tree.Statement. If the func returned by the hook is non-nil, it is used to
// construct a planNode that runs that func in a goroutine during Start.
//
// See PlanHookState comments for information about why plan hooks are needed.
func AddPlanHook(name string, fn planHookFn, typeCheck PlanHookTypeCheckFn) {
	planHooks = append(planHooks, planHook{
		name: name, fn: fn, typeCheck: typeCheck,
	})
}

// ClearPlanHooks is used by tests to clear out any mocked out plan hooks that
// were registered.
func ClearPlanHooks() {
	planHooks = nil
}

// hookFnNode is a planNode implemented in terms of a function. It begins the
// provided function during Start and serves the results it returns over the
// channel.
type hookFnNode struct {
	zeroInputPlanNode
	optColumnsSlot

	name    string
	f       PlanHookRowFn
	header  colinfo.ResultColumns
	stopper *stop.Stopper

	run hookFnRun
}

var _ planNode = &hookFnNode{}

// hookFnRun contains the run-time state of hookFnNode during local execution.
type hookFnRun struct {
	resultsCh chan tree.Datums
	errCh     chan error

	row tree.Datums
}

func newHookFnNode(
	name string, fn PlanHookRowFn, header colinfo.ResultColumns, stopper *stop.Stopper,
) *hookFnNode {
	return &hookFnNode{name: name, f: fn, header: header, stopper: stopper}
}

func (f *hookFnNode) startExec(params runParams) error {
	f.run.resultsCh = make(chan tree.Datums)
	f.run.errCh = make(chan error)
	// Note that it's ok if the async task is not started due to server shutdown
	// because the context should be canceled then too, which would unblock
	// calls to Next if they happen.
	return f.stopper.RunAsyncTaskEx(
		params.ctx,
		stop.TaskOpts{
			TaskName: f.name,
			SpanOpt:  stop.ChildSpan,
		},
		func(ctx context.Context) {
			err := f.f(ctx, f.run.resultsCh)
			select {
			case <-ctx.Done():
			case f.run.errCh <- err:
			}
			close(f.run.errCh)
			close(f.run.resultsCh)
		},
	)
}

func (f *hookFnNode) Next(params runParams) (bool, error) {
	select {
	case <-params.ctx.Done():
		return false, params.ctx.Err()
	case err := <-f.run.errCh:
		return false, err
	case f.run.row = <-f.run.resultsCh:
		return true, nil
	}
}

func (f *hookFnNode) Values() tree.Datums { return f.run.row }

func (f *hookFnNode) Close(ctx context.Context) {}

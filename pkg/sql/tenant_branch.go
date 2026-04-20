// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/zoneconfig"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// branchPTSMetaType is the protected-timestamp meta type for records that
// pin a parent tenant at the fork point of a branch.
const branchPTSMetaType = "branch"

// createTenantAsBranchNode plans CREATE VIRTUAL CLUSTER ... BRANCH FROM. It
// creates a new tenant whose data starts as a copy-on-write view of an
// existing parent tenant: the branch has its own keyspace for writes, but
// reads of unmodified keys fall through to the parent at the branch
// timestamp via a SQL-pod-level Sender wrapper (branchSender).
type createTenantAsBranchNode struct {
	zeroInputPlanNode
	ifNotExists bool
	tenantSpec  tenantSpec
	parentSpec  tenantSpec
}

// CreateTenantAsBranchNode constructs a planNode for a CREATE VIRTUAL CLUSTER
// ... BRANCH FROM statement.
func (p *planner) CreateTenantAsBranchNode(
	ctx context.Context, n *tree.CreateTenantAsBranch,
) (planNode, error) {
	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "CREATE VIRTUAL CLUSTER BRANCH")
	if err != nil {
		return nil, err
	}
	pspec, err := p.planTenantSpec(ctx, n.ParentTenantName, "CREATE VIRTUAL CLUSTER BRANCH")
	if err != nil {
		return nil, err
	}
	return &createTenantAsBranchNode{
		ifNotExists: n.IfNotExists,
		tenantSpec:  tspec,
		parentSpec:  pspec,
	}, nil
}

func (n *createTenantAsBranchNode) startExec(params runParams) error {
	return params.p.createTenantAsBranch(params.ctx, n.tenantSpec, n.parentSpec, n.ifNotExists)
}

func (n *createTenantAsBranchNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *createTenantAsBranchNode) Values() tree.Datums            { return tree.Datums{} }
func (n *createTenantAsBranchNode) Close(_ context.Context)        {}

// createTenantAsBranch creates a branch tenant: it allocates a new tenant ID
// with branch metadata pointing at the parent tenant, writes a protected
// timestamp on the parent's keyspace at the branch fork point, and creates
// an initial split at the branch's keyspace boundary so subsequent writes
// can land.
//
// The branch's SQL pod relies on branchSender (wired in pkg/server/tenant.go)
// to fall through to the parent for any keys the branch has not yet written.
// This includes system tables: we deliberately do not bootstrap a fresh
// keyspace for the branch.
func (p *planner) createTenantAsBranch(
	ctx context.Context, branchSpec, parentSpec tenantSpec, ifNotExists bool,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("CREATE VIRTUAL CLUSTER BRANCH")
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.ExecCfg().Codec, "create", p.ExecCfg().Settings); err != nil {
		return err
	}
	if err := CanManageTenant(ctx, p); err != nil {
		return err
	}

	// Resolve the parent tenant (must exist).
	parentInfo, err := parentSpec.getTenantInfo(ctx, p)
	if err != nil {
		return errors.Wrap(err, "resolving parent tenant")
	}
	parentTID, err := roachpb.MakeTenantID(parentInfo.ID)
	if err != nil {
		return err
	}
	if parentInfo.IsBranch() {
		// Branching off a branch isn't supported in the PoC: branchSender
		// only does a single fallback hop. See .memory/WHITEBOARD.md.
		return errors.UnimplementedErrorf(
			errors.IssueLink{}, "branching from a branch tenant is not yet supported")
	}

	// Resolve the branch's name (and optional ID).
	_, branchName, err := branchSpec.getTenantParameters(ctx, p)
	if err != nil {
		return err
	}

	// Snapshot the current time as the branch fork point. Subsequent reads on
	// the branch that miss in the branch keyspace will read the parent at
	// this timestamp.
	branchTS := p.ExecCfg().Clock.Now()

	var info mtinfopb.TenantInfoWithUsage
	info.Name = branchName
	// Skip the ADD bootstrap state: branchSender will satisfy reads against
	// the parent until the branch writes its own copies.
	info.DataState = mtinfopb.DataStateReady
	info.ServiceMode = mtinfopb.ServiceModeNone
	info.BranchParentID = &parentTID
	info.BranchTimestamp = branchTS

	initialZC, err := zoneconfig.GetHydratedForTenantsRange(ctx, p.Txn(), p.Descriptors())
	if err != nil {
		return err
	}

	branchTID, err := CreateTenantRecord(
		ctx,
		p.ExecCfg().Codec,
		p.ExecCfg().Settings,
		p.InternalSQLTxn(),
		p.ExecCfg().SpanConfigKVAccessor.WithISQLTxn(ctx, p.InternalSQLTxn()),
		&info,
		initialZC,
		ifNotExists,
		p.ExecCfg().TenantTestingKnobs,
	)
	if err != nil {
		return err
	}
	if !branchTID.IsSet() {
		// IF NOT EXISTS hit an existing tenant; nothing else to do.
		return nil
	}

	// Pin the parent's keyspace at branch_ts so reads during the branch's
	// lifetime are not GC'd. The PoC never releases this record; production
	// would need a TTL or branch-deletion path. See .memory/WHITEBOARD.md.
	recordID := uuid.MakeV4()
	ptsRecord := &ptpb.Record{
		ID:        recordID.GetBytesMut(),
		Timestamp: branchTS,
		Mode:      ptpb.PROTECT_AFTER,
		MetaType:  branchPTSMetaType,
		Meta:      []byte(branchTID.String()),
		Target:    ptpb.MakeTenantsTarget([]roachpb.TenantID{parentTID}),
	}
	pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(p.InternalSQLTxn())
	if err := pts.Protect(ctx, ptsRecord); err != nil {
		return errors.Wrap(err, "protecting parent tenant timestamp")
	}

	// Pre-create at least one range in the branch's keyspace so writes have
	// a place to land. Done non-transactionally; a 1h split expiration is
	// fine because subsequent activity will keep the range alive.
	branchSpan := keys.MakeTenantSpan(branchTID)
	expTime := p.ExecCfg().Clock.Now().Add(time.Hour.Nanoseconds(), 0)
	if err := p.ExecCfg().DB.AdminSplit(ctx, branchSpan.Key, expTime); err != nil {
		return errors.Wrap(err, "splitting at branch keyspace boundary")
	}

	return nil
}

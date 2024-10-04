// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file lives here instead of sql/flowinfra to avoid an import cycle.

package execinfra

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantcpu"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/evalcatalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// FlowCtx encompasses the configuration parameters needed for various flow
// components.
type FlowCtx struct {
	AmbientContext log.AmbientContext

	Cfg *ServerConfig

	// ID is a unique identifier for a flow.
	ID execinfrapb.FlowID

	// EvalCtx is used by all the processors in the flow to evaluate expressions.
	// Processors that intend to evaluate expressions with this EvalCtx should
	// get a copy with NewEvalCtx instead of storing a pointer to this one
	// directly (since some processor mutate the EvalContext they use).
	//
	// TODO(andrei): Get rid of this field and pass a non-shared EvalContext to
	// cores of the processors that need it.
	EvalCtx *eval.Context

	Mon *mon.BytesMonitor

	// The transaction in which kv operations performed by processors in the flow
	// must be performed. Processors in the Flow will use this txn concurrently.
	// This field is generally not nil, except for flows that don't run in a
	// higher-level txn (like backfills).
	Txn *kv.Txn

	// MakeLeafTxn returns a new LeafTxn, different from Txn.
	MakeLeafTxn func() (*kv.Txn, error)

	// Descriptors is used to look up leased table descriptors and to construct
	// transaction bound TypeResolvers to resolve type references during flow
	// setup. It is not safe for concurrent use and is intended to be used only
	// during flow setup and initialization. The Descriptors object is initialized
	// when the FlowContext is created on the gateway node using the planner's
	// descs.Collection and is created on remote nodes with a new descs.Collection
	// In the latter case, after the flow is complete, all descriptors leased from
	// this object must be released.
	Descriptors *descs.Collection

	// EvalCatalogBuiltins is initialized if the flow context is remote and the
	// above descs.Collection is non-nil. It is referenced in the eval.Context
	// in order to provide catalog access to builtins.
	EvalCatalogBuiltins evalcatalog.Builtins

	// IsDescriptorsCleanupRequired is set if Descriptors needs to release the
	// leases it acquired after the flow is complete.
	IsDescriptorsCleanupRequired bool

	// nodeID is the ID of the node on which the processors using this FlowCtx
	// run.
	NodeID *base.SQLIDContainer

	// TraceKV is true if KV tracing was requested by the session.
	TraceKV bool

	// CollectStats is true if execution stats collection was requested.
	CollectStats bool

	// Local is true if this flow is being run as part of a local-only query.
	Local bool

	// Gateway is true if this flow is being run on the gateway node.
	Gateway bool

	// DiskMonitor is this flow's disk monitor. All disk usage for this flow must
	// be registered through this monitor.
	DiskMonitor *mon.BytesMonitor

	// TenantCPUMonitor is used to estimate a query's CPU usage for tenants
	// running EXPLAIN ANALYZE. Currently, it is only used by remote flows.
	// The gateway flow is handled by the connExecutor.
	TenantCPUMonitor multitenantcpu.CPUUsageHelper
}

// NewEvalCtx returns a modifiable copy of the FlowCtx's EvalContext.
// Processors should use this method any time they need to store a pointer to
// the EvalContext, since processors may mutate the EvalContext. Specifically,
// every processor that runs ProcOutputHelper.Init must pass in a modifiable
// EvalContext, since it stores that EvalContext in its exprHelpers and mutates
// them at runtime to ensure expressions are evaluated with the correct indexed
// var context.
// TODO(yuzefovich): once we remove eval.Context.deprecatedContext, re-evaluate
// this since many processors don't modify the eval context except for that
// field.
func (flowCtx *FlowCtx) NewEvalCtx() *eval.Context {
	evalCopy := flowCtx.EvalCtx.Copy()
	return evalCopy
}

// TestingKnobs returns the distsql testing knobs for this flow context.
func (flowCtx *FlowCtx) TestingKnobs() TestingKnobs {
	return flowCtx.Cfg.TestingKnobs
}

// Stopper returns the stopper for this flowCtx.
func (flowCtx *FlowCtx) Stopper() *stop.Stopper {
	return flowCtx.Cfg.Stopper
}

// Codec returns the SQL codec for this flowCtx.
func (flowCtx *FlowCtx) Codec() keys.SQLCodec {
	return flowCtx.EvalCtx.Codec
}

// TableDescriptor returns a catalog.TableDescriptor object for the given
// descriptor proto, using the descriptors collection if it is available.
func (flowCtx *FlowCtx) TableDescriptor(
	ctx context.Context, desc *descpb.TableDescriptor,
) catalog.TableDescriptor {
	if desc == nil {
		return nil
	}
	if flowCtx != nil && flowCtx.Descriptors != nil && flowCtx.Txn != nil {
		leased, _ := flowCtx.Descriptors.GetLeasedImmutableTableByID(ctx, flowCtx.Txn, desc.ID)
		if leased != nil && leased.GetVersion() == desc.Version {
			return leased
		}
	}
	return tabledesc.NewUnsafeImmutable(desc)
}

// NewTypeResolver creates a new TypeResolver that is bound under the input
// transaction. It returns a nil resolver if the FlowCtx doesn't hold a
// descs.Collection object.
func (flowCtx *FlowCtx) NewTypeResolver(txn *kv.Txn) descs.DistSQLTypeResolver {
	if flowCtx == nil || flowCtx.Descriptors == nil {
		return descs.DistSQLTypeResolver{}
	}
	return descs.NewDistSQLTypeResolver(flowCtx.Descriptors, txn)
}

// NewSemaContext creates a new SemaContext with a TypeResolver bound to the
// input transaction.
func (flowCtx *FlowCtx) NewSemaContext(txn *kv.Txn) *tree.SemaContext {
	resolver := flowCtx.NewTypeResolver(txn)
	semaCtx := tree.MakeSemaContext()
	semaCtx.TypeResolver = &resolver
	return &semaCtx
}

// ProcessorComponentID returns a ComponentID for the given processor in this
// flow.
func (flowCtx *FlowCtx) ProcessorComponentID(procID int32) execinfrapb.ComponentID {
	return execinfrapb.ProcessorComponentID(flowCtx.NodeID.SQLInstanceID(), flowCtx.ID, procID)
}

// StreamComponentID returns a ComponentID for the given stream in this flow.
// The stream must originate from the node associated with this FlowCtx.
func (flowCtx *FlowCtx) StreamComponentID(streamID execinfrapb.StreamID) execinfrapb.ComponentID {
	return execinfrapb.StreamComponentID(flowCtx.NodeID.SQLInstanceID(), flowCtx.ID, streamID)
}

// FlowComponentID returns a ComponentID for the given flow.
func (flowCtx *FlowCtx) FlowComponentID() execinfrapb.ComponentID {
	region, _ := flowCtx.Cfg.Locality.Find("region")
	return execinfrapb.FlowComponentID(flowCtx.NodeID.SQLInstanceID(), flowCtx.ID, region)
}

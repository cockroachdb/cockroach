// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// ConvertToColumnOrdering converts an Ordering type (as defined in data.proto)
// to a sqlbase.ColumnOrdering type.
func ConvertToColumnOrdering(specOrdering Ordering) sqlbase.ColumnOrdering {
	ordering := make(sqlbase.ColumnOrdering, len(specOrdering.Columns))
	for i, c := range specOrdering.Columns {
		ordering[i].ColIdx = int(c.ColIdx)
		if c.Direction == Ordering_Column_ASC {
			ordering[i].Direction = encoding.Ascending
		} else {
			ordering[i].Direction = encoding.Descending
		}
	}
	return ordering
}

// ConvertToSpecOrdering converts a sqlbase.ColumnOrdering type
// to an Ordering type (as defined in data.proto).
func ConvertToSpecOrdering(columnOrdering sqlbase.ColumnOrdering) Ordering {
	return ConvertToMappedSpecOrdering(columnOrdering, nil)
}

// ConvertToMappedSpecOrdering converts a sqlbase.ColumnOrdering type
// to an Ordering type (as defined in data.proto), using the column
// indices contained in planToStreamColMap.
func ConvertToMappedSpecOrdering(
	columnOrdering sqlbase.ColumnOrdering, planToStreamColMap []int,
) Ordering {
	specOrdering := Ordering{}
	specOrdering.Columns = make([]Ordering_Column, len(columnOrdering))
	for i, c := range columnOrdering {
		colIdx := c.ColIdx
		if planToStreamColMap != nil {
			colIdx = planToStreamColMap[c.ColIdx]
			if colIdx == -1 {
				panic(fmt.Sprintf("column %d in sort ordering not available", c.ColIdx))
			}
		}
		specOrdering.Columns[i].ColIdx = uint32(colIdx)
		if c.Direction == encoding.Ascending {
			specOrdering.Columns[i].Direction = Ordering_Column_ASC
		} else {
			specOrdering.Columns[i].Direction = Ordering_Column_DESC
		}
	}
	return specOrdering
}

// DistSQLTypeResolver implements tree.ResolvableTypeReference for accessing
// type information during DistSQL query evaluation.
type DistSQLTypeResolver struct {
	EvalContext *tree.EvalContext
	// TODO (rohany): This struct should locally cache id -> types.T here
	//  so that repeated lookups do not incur additional KV operations.
}

// ResolveType implements tree.ResolvableTypeReference.
func (tr *DistSQLTypeResolver) ResolveType(
	context.Context, *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.AssertionFailedf("cannot resolve types in DistSQL by name")
}

func makeTypeLookupFunc(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) sqlbase.TypeLookupFunc {
	return func(id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
		return resolver.ResolveTypeDescByID(ctx, txn, codec, id, tree.ObjectLookupFlags{})
	}
}

// ResolveTypeByID implements tree.ResolvableTypeReference.
func (tr *DistSQLTypeResolver) ResolveTypeByID(ctx context.Context, id uint32) (*types.T, error) {
	// TODO (rohany): This should eventually look into the set of cached type
	//  descriptors before attempting to access it here.
	lookup := makeTypeLookupFunc(ctx, tr.EvalContext.Txn, tr.EvalContext.Codec)
	name, typDesc, err := lookup(sqlbase.ID(id))
	if err != nil {
		return nil, err
	}
	return typDesc.MakeTypesT(name, lookup)
}

// HydrateTypeSlice hydrates all user defined types in an input slice of types.
func HydrateTypeSlice(evalCtx *tree.EvalContext, typs []*types.T) error {
	// TODO (rohany): This should eventually look into the set of cached type
	//  descriptors before attempting to access it here.
	lookup := makeTypeLookupFunc(evalCtx.Context, evalCtx.Txn, evalCtx.Codec)
	for _, t := range typs {
		if t.UserDefined() {
			name, typDesc, err := lookup(sqlbase.ID(t.StableTypeID()))
			if err != nil {
				return err
			}
			if err := typDesc.HydrateTypeInfoWithName(t, name, lookup); err != nil {
				return err
			}
		}
	}
	return nil
}

// ExprFmtCtxBase produces a FmtCtx used for serializing expressions; a proper
// IndexedVar formatting function needs to be added on. It replaces placeholders
// with their values.
func ExprFmtCtxBase(evalCtx *tree.EvalContext) *tree.FmtCtx {
	fmtCtx := tree.NewFmtCtx(tree.FmtCheckEquivalence)
	fmtCtx.SetPlaceholderFormat(
		func(fmtCtx *tree.FmtCtx, p *tree.Placeholder) {
			d, err := p.Eval(evalCtx)
			if err != nil {
				panic(fmt.Sprintf("failed to serialize placeholder: %s", err))
			}
			d.Format(fmtCtx)
		})
	return fmtCtx
}

// Expression is the representation of a SQL expression.
// See data.proto for the corresponding proto definition. Its automatic type
// declaration is suppressed in the proto via the typedecl=false option, so that
// we can add the LocalExpr field which is not serialized. It never needs to be
// serialized because we only use it in the case where we know we won't need to
// send it, as a proto, to another machine.
type Expression struct {
	// Version is unused.
	Version string

	// Expr, if present, is the string representation of this expression.
	// SQL expressions are passed as a string, with ordinal references
	// (@1, @2, @3 ..) used for "input" variables.
	Expr string

	// LocalExpr is an unserialized field that's used to pass expressions to
	// the gateway node without serializing/deserializing them. It is always
	// set in non-test setup.
	LocalExpr tree.TypedExpr
}

// Empty returns true if the expression has neither an Expr nor LocalExpr.
func (e *Expression) Empty() bool {
	return e.Expr == "" && e.LocalExpr == nil
}

// String implements the Stringer interface.
func (e Expression) String() string {
	if e.Expr != "" {
		return e.Expr
	}
	if e.LocalExpr != nil {
		ctx := tree.NewFmtCtx(tree.FmtCheckEquivalence)
		ctx.FormatNode(e.LocalExpr)
		return ctx.CloseAndGetString()
	}
	return "none"
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	if err := e.ErrorDetail(context.TODO()); err != nil {
		return err.Error()
	}
	return "<nil>"
}

// NewError creates an Error from an error, to be sent on the wire. It will
// recognize certain errors and marshall them accordingly, and everything
// unrecognized is turned into a PGError with code "internal".
func NewError(ctx context.Context, err error) *Error {
	resErr := &Error{}

	// Encode the full error to the best of our ability.
	// This field is ignored by 19.1 nodes and prior.
	ctx = logtags.AddTag(ctx, "sent-error", nil)
	fullError := errors.EncodeError(ctx, errors.WithContextTags(err, ctx))
	resErr.FullError = &fullError

	return resErr
}

// ErrorDetail returns the payload as a Go error.
func (e *Error) ErrorDetail(ctx context.Context) (err error) {
	if e == nil {
		return nil
	}
	defer func() {
		ctx = logtags.AddTag(ctx, "received-error", nil)
		err = errors.WithContextTags(err, ctx)
	}()

	if e.FullError != nil {
		// If there's a 19.2-forward full error, decode and use that.
		// This will reveal a fully causable detailed error structure.
		return errors.DecodeError(ctx, *e.FullError)
	}

	// We're receiving an error we don't know about. It's all right,
	// it's still an error, just one we didn't expect. Let it go
	// through. We'll pick it up in reporting.
	return errors.AssertionFailedf("unknown error from remote node")
}

// ProducerMetadata represents a metadata record flowing through a DistSQL flow.
type ProducerMetadata struct {
	// Only one of these fields will be set. If this ever changes, note that
	// there're consumers out there that extract the error and, if there is one,
	// forward it in isolation and drop the rest of the record.
	Ranges []roachpb.RangeInfo
	// TODO(vivek): change to type Error
	Err error
	// TraceData is sent if snowball tracing is enabled.
	TraceData []tracing.RecordedSpan
	// LeafTxnFinalState contains the final state of the LeafTxn to be
	// sent from leaf flows to the RootTxn held by the flow's ultimate
	// receiver.
	LeafTxnFinalState *roachpb.LeafTxnFinalState
	// RowNum corresponds to a row produced by a "source" processor that takes no
	// inputs. It is used in tests to verify that all metadata is forwarded
	// exactly once to the receiver on the gateway node.
	RowNum *RemoteProducerMetadata_RowNum
	// SamplerProgress contains incremental progress information from the sampler
	// processor.
	SamplerProgress *RemoteProducerMetadata_SamplerProgress
	// BulkProcessorProgress contains incremental progress information from a bulk
	// operation processor (backfiller, import, etc).
	BulkProcessorProgress *RemoteProducerMetadata_BulkProcessorProgress
	// Metrics contains information about goodput of the node.
	Metrics *RemoteProducerMetadata_Metrics
}

var (
	// TODO(yuzefovich): use this pool in other places apart from metrics
	// collection.
	// producerMetadataPool is a pool of producer metadata objects.
	producerMetadataPool = sync.Pool{
		New: func() interface{} {
			return &ProducerMetadata{}
		},
	}

	// rpmMetricsPool is a pool of metadata used to propagate metrics.
	rpmMetricsPool = sync.Pool{
		New: func() interface{} {
			return &RemoteProducerMetadata_Metrics{}
		},
	}
)

// Release is part of Releasable interface.
func (meta *ProducerMetadata) Release() {
	*meta = ProducerMetadata{}
	producerMetadataPool.Put(meta)
}

// Release is part of Releasable interface. Note that although this meta is
// only used together with a ProducerMetadata that comes from another pool, we
// do not combine two Release methods into one because two objects have a
// different lifetime.
func (meta *RemoteProducerMetadata_Metrics) Release() {
	*meta = RemoteProducerMetadata_Metrics{}
	rpmMetricsPool.Put(meta)
}

// GetProducerMeta returns a producer metadata object from the pool.
func GetProducerMeta() *ProducerMetadata {
	return producerMetadataPool.Get().(*ProducerMetadata)
}

// GetMetricsMeta returns a metadata object from the pool of metrics metadata.
func GetMetricsMeta() *RemoteProducerMetadata_Metrics {
	return rpmMetricsPool.Get().(*RemoteProducerMetadata_Metrics)
}

// RemoteProducerMetaToLocalMeta converts a RemoteProducerMetadata struct to
// ProducerMetadata and returns whether the conversion was successful or not.
func RemoteProducerMetaToLocalMeta(
	ctx context.Context, rpm RemoteProducerMetadata,
) (ProducerMetadata, bool) {
	meta := GetProducerMeta()
	switch v := rpm.Value.(type) {
	case *RemoteProducerMetadata_RangeInfo:
		meta.Ranges = v.RangeInfo.RangeInfo
	case *RemoteProducerMetadata_TraceData_:
		meta.TraceData = v.TraceData.CollectedSpans
	case *RemoteProducerMetadata_LeafTxnFinalState:
		meta.LeafTxnFinalState = v.LeafTxnFinalState
	case *RemoteProducerMetadata_RowNum_:
		meta.RowNum = v.RowNum
	case *RemoteProducerMetadata_SamplerProgress_:
		meta.SamplerProgress = v.SamplerProgress
	case *RemoteProducerMetadata_BulkProcessorProgress_:
		meta.BulkProcessorProgress = v.BulkProcessorProgress
	case *RemoteProducerMetadata_Error:
		meta.Err = v.Error.ErrorDetail(ctx)
	case *RemoteProducerMetadata_Metrics_:
		meta.Metrics = v.Metrics
	default:
		return *meta, false
	}
	return *meta, true
}

// LocalMetaToRemoteProducerMeta converts a ProducerMetadata struct to
// RemoteProducerMetadata.
func LocalMetaToRemoteProducerMeta(
	ctx context.Context, meta ProducerMetadata,
) RemoteProducerMetadata {
	var rpm RemoteProducerMetadata
	if meta.Ranges != nil {
		rpm.Value = &RemoteProducerMetadata_RangeInfo{
			RangeInfo: &RemoteProducerMetadata_RangeInfos{
				RangeInfo: meta.Ranges,
			},
		}
	} else if meta.TraceData != nil {
		rpm.Value = &RemoteProducerMetadata_TraceData_{
			TraceData: &RemoteProducerMetadata_TraceData{
				CollectedSpans: meta.TraceData,
			},
		}
	} else if meta.LeafTxnFinalState != nil {
		rpm.Value = &RemoteProducerMetadata_LeafTxnFinalState{
			LeafTxnFinalState: meta.LeafTxnFinalState,
		}
	} else if meta.RowNum != nil {
		rpm.Value = &RemoteProducerMetadata_RowNum_{
			RowNum: meta.RowNum,
		}
	} else if meta.SamplerProgress != nil {
		rpm.Value = &RemoteProducerMetadata_SamplerProgress_{
			SamplerProgress: meta.SamplerProgress,
		}
	} else if meta.BulkProcessorProgress != nil {
		rpm.Value = &RemoteProducerMetadata_BulkProcessorProgress_{
			BulkProcessorProgress: meta.BulkProcessorProgress,
		}
	} else if meta.Metrics != nil {
		rpm.Value = &RemoteProducerMetadata_Metrics_{
			Metrics: meta.Metrics,
		}
	} else {
		rpm.Value = &RemoteProducerMetadata_Error{
			Error: NewError(ctx, meta.Err),
		}
	}
	return rpm
}

// MetadataSource is an interface implemented by processors and columnar
// operators that can produce metadata.
type MetadataSource interface {
	// DrainMeta returns all the metadata produced by the processor or operator.
	// It will be called exactly once, usually, when the processor or operator
	// has finished doing its computations. This is a signal that the output
	// requires no more rows to be returned.
	// Implementers can choose what to do on subsequent calls (if such occur).
	// TODO(yuzefovich): modify the contract to require returning nil on all
	// calls after the first one.
	DrainMeta(context.Context) []ProducerMetadata
}

// MetadataSources is a slice of MetadataSource.
type MetadataSources []MetadataSource

// DrainMeta calls DrainMeta on all MetadataSources and returns a single slice
// with all the accumulated metadata.
func (s MetadataSources) DrainMeta(ctx context.Context) []ProducerMetadata {
	var result []ProducerMetadata
	for _, src := range s {
		result = append(result, src.DrainMeta(ctx)...)
	}
	return result
}

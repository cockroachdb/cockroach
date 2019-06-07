// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlpb

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
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

	// LocalExpr is an unserialized field that's used to pass expressions to local
	// flows without serializing/deserializing them.
	LocalExpr tree.TypedExpr
}

// Empty returns true if the expression has neither an Expr nor LocalExpr.
func (e *Expression) Empty() bool {
	return e.Expr == "" && e.LocalExpr == nil
}

// String implements the Stringer interface.
func (e Expression) String() string {
	if e.LocalExpr != nil {
		ctx := tree.NewFmtCtx(tree.FmtCheckEquivalence)
		ctx.FormatNode(e.LocalExpr)
		return ctx.CloseAndGetString()
	}
	if e.Expr != "" {
		return e.Expr
	}
	return "none"
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	if err := e.ErrorDetail(); err != nil {
		return err.Error()
	}
	return "<nil>"
}

// NewError creates an Error from an error, to be sent on the wire. It will
// recognize certain errors and marshall them accordingly, and everything
// unrecognized is turned into a PGError with code "internal".
func NewError(err error) *Error {
	// Unwrap the error, to attain the cause.
	// Otherwise, Wrap() may hide the roachpb error
	// from the cast attempt below.
	origErr := err
	err = errors.Cause(err)

	if pgErr, ok := pgerror.GetPGCause(err); ok {
		return &Error{Detail: &Error_PGError{PGError: pgErr}}
	}

	switch e := err.(type) {
	case *roachpb.UnhandledRetryableError:
		return &Error{Detail: &Error_RetryableTxnError{RetryableTxnError: e}}
	case *roachpb.NodeUnavailableError:
		// Node failures are common enough that we shouldn't fail with
		// assertion errors upon them. Simply signal them in a way that
		// may make sense to a client.
		return &Error{
			Detail: &Error_PGError{
				PGError: pgerror.Newf(pgerror.CodeRangeUnavailable, "%v", e),
			},
		}
	default:
		// Anything unrecognized is an "internal error".
		return &Error{
			Detail: &Error_PGError{
				PGError: pgerror.AssertionFailedf(
					"uncaught error: %+v", origErr)}}
	}
}

// ErrorDetail returns the payload as a Go error.
func (e *Error) ErrorDetail() error {
	if e == nil {
		return nil
	}
	switch t := e.Detail.(type) {
	case *Error_PGError:
		return t.PGError
	case *Error_RetryableTxnError:
		return t.RetryableTxnError
	default:
		// We're receiving an error we don't know about. It's all right,
		// it's still an error, just one we didn't expect. Let it go
		// through. We'll pick it up in reporting.
		return pgerror.AssertionFailedf("unknown error detail type: %+v", t)
	}
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
	// TxnCoordMeta contains the updated transaction coordinator metadata,
	// to be sent from leaf transactions to augment the root transaction,
	// held by the flow's ultimate receiver.
	TxnCoordMeta *roachpb.TxnCoordMeta
	// RowNum corresponds to a row produced by a "source" processor that takes no
	// inputs. It is used in tests to verify that all metadata is forwarded
	// exactly once to the receiver on the gateway node.
	RowNum *RemoteProducerMetadata_RowNum
	// SamplerProgress contains incremental progress information from the sampler
	// processor.
	SamplerProgress *RemoteProducerMetadata_SamplerProgress
	// Metrics contains information about goodput of the node.
	Metrics *RemoteProducerMetadata_Metrics
}

// metricsMetaPool is a pool of metadata used to propagate metrics.
var metricsMetaPool = sync.Pool{
	New: func() interface{} {
		return &ProducerMetadata{
			Metrics: &RemoteProducerMetadata_Metrics{},
		}
	},
}

// Release is part of Releasable interface. Note that it is assumed that meta
// was obtained from metricsMetaPool, so only metrics fields are reset.
func (meta *ProducerMetadata) Release() {
	meta.Metrics.BytesRead = 0
	meta.Metrics.RowsRead = 0
	metricsMetaPool.Put(meta)
}

// GetMetricsMeta returns a metadata object from the pool of metrics metadata.
func GetMetricsMeta() *ProducerMetadata {
	return metricsMetaPool.Get().(*ProducerMetadata)
}

// RemoteProducerMetaToLocalMeta converts a RemoteProducerMetadata struct to
// ProducerMetadata and returns whether the conversion was successful or not.
func RemoteProducerMetaToLocalMeta(rpm RemoteProducerMetadata) (ProducerMetadata, bool) {
	var meta ProducerMetadata
	switch v := rpm.Value.(type) {
	case *RemoteProducerMetadata_RangeInfo:
		meta.Ranges = v.RangeInfo.RangeInfo
	case *RemoteProducerMetadata_TraceData_:
		meta.TraceData = v.TraceData.CollectedSpans
	case *RemoteProducerMetadata_TxnCoordMeta:
		meta.TxnCoordMeta = v.TxnCoordMeta
	case *RemoteProducerMetadata_RowNum_:
		meta.RowNum = v.RowNum
	case *RemoteProducerMetadata_SamplerProgress_:
		meta.SamplerProgress = v.SamplerProgress
	case *RemoteProducerMetadata_Error:
		meta.Err = v.Error.ErrorDetail()
	case *RemoteProducerMetadata_Metrics_:
		meta.Metrics = v.Metrics
	default:
		return meta, false
	}
	return meta, true
}

// LocalMetaToRemoteProducerMeta converts a ProducerMetadata struct to
// RemoteProducerMetadata.
func LocalMetaToRemoteProducerMeta(meta ProducerMetadata) RemoteProducerMetadata {
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
	} else if meta.TxnCoordMeta != nil {
		rpm.Value = &RemoteProducerMetadata_TxnCoordMeta{
			TxnCoordMeta: meta.TxnCoordMeta,
		}
	} else if meta.RowNum != nil {
		rpm.Value = &RemoteProducerMetadata_RowNum_{
			RowNum: meta.RowNum,
		}
	} else if meta.SamplerProgress != nil {
		rpm.Value = &RemoteProducerMetadata_SamplerProgress_{
			SamplerProgress: meta.SamplerProgress,
		}
	} else if meta.Metrics != nil {
		rpm.Value = &RemoteProducerMetadata_Metrics_{
			Metrics: meta.Metrics,
		}
	} else {
		rpm.Value = &RemoteProducerMetadata_Error{
			Error: NewError(meta.Err),
		}
	}
	return rpm
}

// MetadataSource is an interface implemented by processors and columnar
// operators that can produce metadata.
type MetadataSource interface {
	// DrainMeta returns all the metadata produced by the processor or operator.
	// It will be called exactly once, usually, when the processor or operator
	// has finished doing its computations.
	// Implementers can choose what to do on subsequent calls (if such occur).
	// TODO(yuzefovich): modify the contract to require returning nil on all
	// calls after the first one.
	DrainMeta(context.Context) []ProducerMetadata
}

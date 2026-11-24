// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physicalplan

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func RoutingKeyForSQLInstance(sqlInstanceID base.SQLInstanceID) roachpb.Key {
	return roachpb.Key(fmt.Sprintf("node%d", sqlInstanceID))
}

// RoutingDatumsForSQLInstance returns a pair of encoded datums representing the
// start and end keys for routing data to a specific SQL instance. This is used
// when setting up range-based routing in DistSQL physical plans.
func RoutingDatumsForSQLInstance(
	sqlInstanceID base.SQLInstanceID,
) (rowenc.EncDatum, rowenc.EncDatum) {
	routingBytes := RoutingKeyForSQLInstance(sqlInstanceID)
	startDatum := rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes)))
	endDatum := rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes.Next())))
	return startDatum, endDatum
}

// RoutingSpanForSQLInstance provides the encoded byte ranges to be used during
// DistSQL planning when setting up the output router for a specific SQL instance.
func RoutingSpanForSQLInstance(sqlInstanceID base.SQLInstanceID) ([]byte, []byte, error) {
	var alloc tree.DatumAlloc
	startDatum, endDatum := RoutingDatumsForSQLInstance(sqlInstanceID)

	var startBytes, endBytes []byte
	startBytes, err := startDatum.Encode(types.Bytes, &alloc, catenumpb.DatumEncoding_ASCENDING_KEY, startBytes)
	if err != nil {
		return nil, nil, err
	}
	endBytes, err = endDatum.Encode(types.Bytes, &alloc, catenumpb.DatumEncoding_ASCENDING_KEY, endBytes)
	if err != nil {
		return nil, nil, err
	}
	return startBytes, endBytes, nil
}

// MakeInstanceRouter creates a RangeRouterSpec that routes data to SQL instances
// based on routing keys. Each SQL instance gets assigned a span, and the router
// directs rows to the appropriate stream based on which span the routing key falls into.
func MakeInstanceRouter(
	ids []base.SQLInstanceID,
) (execinfrapb.OutputRouterSpec_RangeRouterSpec, error) {
	var zero execinfrapb.OutputRouterSpec_RangeRouterSpec
	rangeRouterSpec := execinfrapb.OutputRouterSpec_RangeRouterSpec{
		Spans: nil,
		// DefaultDest is nil so that any routing key that doesn't match a span
		// will produce an error. This ensures we catch coordination bugs where
		// a routing key is generated for an instance not in the router's span list,
		// rather than silently routing to an arbitrary instance.
		DefaultDest: nil,
		Encodings: []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: catenumpb.DatumEncoding_ASCENDING_KEY,
			},
		},
	}
	for stream, sqlInstanceID := range ids {
		startBytes, endBytes, err := RoutingSpanForSQLInstance(sqlInstanceID)
		if err != nil {
			return zero, err
		}

		span := execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
			Start:  startBytes,
			End:    endBytes,
			Stream: int32(stream),
		}
		rangeRouterSpec.Spans = append(rangeRouterSpec.Spans, span)
	}
	// The router expects the spans to be sorted.
	slices.SortFunc(rangeRouterSpec.Spans, func(a, b execinfrapb.OutputRouterSpec_RangeRouterSpec_Span) int {
		return bytes.Compare(a.Start, b.Start)
	})
	return rangeRouterSpec, nil
}

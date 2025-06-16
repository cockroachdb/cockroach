// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

type protobufEncoder struct {
	updatedField       bool
	mvccTimestampField bool
	beforeField        bool
	keyInValue         bool
	topicInValue       bool
	envelopeType       changefeedbase.EnvelopeType
	targets            changefeedbase.Targets
}

// protobufEncoderOptions wraps EncodingOptions for initializing a protobufEncoder.
type protobufEncoderOptions struct {
	changefeedbase.EncodingOptions
}

var _ Encoder = &protobufEncoder{}

// newProtobufEncoder constructs a new protobufEncoder from the given options and targets.
func newProtobufEncoder(
	ctx context.Context, opts protobufEncoderOptions, targets changefeedbase.Targets,
) Encoder {
	return &protobufEncoder{
		envelopeType:       opts.Envelope,
		keyInValue:         opts.KeyInValue,
		topicInValue:       opts.TopicInValue,
		beforeField:        opts.Diff,
		updatedField:       opts.UpdatedTimestamps,
		mvccTimestampField: opts.MVCCTimestamps,
		targets:            targets,
	}
}

// EncodeKey serializes the primary key columns of a row as a changefeedpb.Key message.
func (e *protobufEncoder) EncodeKey(ctx context.Context, row cdcevent.Row) ([]byte, error) {
	keyMsg, err := buildKeyMessage(row)
	if err != nil {
		return nil, err
	}
	return protoutil.Marshal(keyMsg)
}

// EncodeValue serializes a row event using the configured envelope type.
func (e *protobufEncoder) EncodeValue(
	ctx context.Context, evCtx eventContext, updatedRow, prevRow cdcevent.Row,
) ([]byte, error) {
	switch e.envelopeType {
	case changefeedbase.OptEnvelopeBare:
		return e.buildBare(evCtx, updatedRow, prevRow)
	default:
		return nil, errors.AssertionFailedf("envelope format not supported: %s", e.envelopeType)
	}
}

// EncodeResolvedTimestamp encodes a resolved timestamp message for the specified topic.
func (e *protobufEncoder) EncodeResolvedTimestamp(
	context.Context, string, hlc.Timestamp,
) ([]byte, error) {
	return nil, unimplemented.NewWithIssuef(148934, "protobuf encoder does not support resolved timestamps yet")
}

// buildBare constructs a BareEnvelope with optional metadata and serializes it.
func (e *protobufEncoder) buildBare(
	evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	after, err := encodeRowToRecord(updatedRow)
	if err != nil {
		return nil, err
	}

	meta, err := e.buildMetadata(evCtx, updatedRow)
	if err != nil {
		return nil, err
	}

	env := &changefeedpb.Message{
		Data: &changefeedpb.Message_Bare{
			Bare: &changefeedpb.BareEnvelope{
				Values:  after.Values,
				XCrdb__: meta,
			},
		},
	}
	return protoutil.Marshal(env)
}

// buildMetadata returns metadata to include in the BareEnvelope.
func (e *protobufEncoder) buildMetadata(
	evCtx eventContext, row cdcevent.Row,
) (*changefeedpb.Metadata, error) {
	if !e.updatedField && !e.mvccTimestampField && !e.keyInValue && !e.topicInValue {
		return nil, nil
	}
	meta := &changefeedpb.Metadata{}
	if e.updatedField {
		meta.Updated = evCtx.updated.AsOfSystemTime()
	}
	if e.mvccTimestampField {
		meta.MvccTimestamp = evCtx.mvcc.AsOfSystemTime()
	}
	if e.keyInValue {
		key, err := buildKeyMessage(row)
		if err != nil {
			return nil, err
		}
		meta.Key = key
	}
	if e.topicInValue {
		meta.Topic = evCtx.topic
	}
	return meta, nil
}

// encodeRowToRecord converts a Row into a Record proto.
func encodeRowToRecord(row cdcevent.Row) (*changefeedpb.Record, error) {
	record := &changefeedpb.Record{Values: make(map[string]*changefeedpb.Value, row.NumValueColumns())}
	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		val, err := datumToProtoValue(d)
		if err != nil {
			return err
		}
		record.Values[col.Name] = val
		return nil
	}); err != nil {
		return nil, err
	}
	return record, nil
}

// buildKeyMessage encodes primary key columns as a Key proto message.
func buildKeyMessage(row cdcevent.Row) (*changefeedpb.Key, error) {
	keyMap := make(map[string]*changefeedpb.Value, row.NumKeyColumns())

	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		val, err := datumToProtoValue(d)
		if err != nil {
			return err
		}
		keyMap[col.Name] = val
		return nil
	}); err != nil {
		return nil, err
	}
	return &changefeedpb.Key{Key: keyMap}, nil
}

// datumToProtoValue converts a tree.Datum into a changefeedpb.Value.
// It handles all common CockroachDB datum types and maps them to their
// corresponding protobuf representation.
func datumToProtoValue(d tree.Datum) (*changefeedpb.Value, error) {
	d = tree.UnwrapDOidWrapper(d)
	if d == tree.DNull {
		return nil, nil
	}
	switch v := d.(type) {
	case *tree.DBool:
		return &changefeedpb.Value{Value: &changefeedpb.Value_BoolValue{BoolValue: bool(*v)}}, nil
	case *tree.DInt:
		return &changefeedpb.Value{Value: &changefeedpb.Value_Int64Value{Int64Value: int64(*v)}}, nil
	case *tree.DFloat:
		return &changefeedpb.Value{Value: &changefeedpb.Value_DoubleValue{DoubleValue: float64(*v)}}, nil
	case *tree.DString:
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: string(*v)}}, nil
	case *tree.DDecimal:
		//TODO(#149711): improve decimal value encoding
		return &changefeedpb.Value{Value: &changefeedpb.Value_DecimalValue{DecimalValue: &changefeedpb.Decimal{Value: v.Decimal.String()}}}, nil
	case *tree.DEnum:
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: v.LogicalRep}}, nil
	case *tree.DCollatedString:
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: v.Contents}}, nil
	case *tree.DJSON:
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: v.JSON.String()}}, nil
	case *tree.DArray:
		elems := make([]*changefeedpb.Value, 0, v.Len())
		for _, elt := range v.Array {
			pv, err := datumToProtoValue(elt)
			if err != nil {
				return nil, err
			}
			elems = append(elems, pv)
		}
		return &changefeedpb.Value{
			Value: &changefeedpb.Value_ArrayValue{ArrayValue: &changefeedpb.Array{Values: elems}}}, nil
	case *tree.DTuple:
		labels := v.ResolvedType().TupleLabels()
		records := make(map[string]*changefeedpb.Value, len(v.D))
		for i, elem := range v.D {
			pv, err := datumToProtoValue(elem)
			if err != nil {
				return nil, err
			}
			var label string
			if i >= len(labels) || labels[i] == "" {
				label = fmt.Sprintf("f%d", i+1)
			} else {
				label = labels[i]
			}
			records[label] = pv
		}
		return &changefeedpb.Value{
			Value: &changefeedpb.Value_TupleValue{
				TupleValue: &changefeedpb.Record{
					Values: records,
				},
			},
		}, nil
	case *tree.DTimestampTZ:
		ts, err := types.TimestampProto(v.Time)
		if err != nil {
			return nil, err
		}
		return &changefeedpb.Value{Value: &changefeedpb.Value_TimestampValue{TimestampValue: ts}}, nil
	case *tree.DTimestamp:
		ts, err := types.TimestampProto(v.Time.UTC())
		if err != nil {
			return nil, err
		}
		return &changefeedpb.Value{Value: &changefeedpb.Value_TimestampValue{TimestampValue: ts}}, nil
	case *tree.DBytes:
		return &changefeedpb.Value{Value: &changefeedpb.Value_BytesValue{BytesValue: []byte(*v)}}, nil

	case *tree.DGeography:
		geostr, err := geo.SpatialObjectToWKT(v.Geography.SpatialObject(), -1)
		if err != nil {
			return nil, err
		}
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: string(geostr)}}, nil
	case *tree.DGeometry:
		geostr, err := geo.SpatialObjectToWKT(v.Geometry.SpatialObject(), -1)
		if err != nil {
			return nil, err
		}
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: string(geostr)}}, nil
	case *tree.DVoid:
		return nil, nil
	case *tree.DDate:
		date, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return &changefeedpb.Value{Value: &changefeedpb.Value_DateValue{DateValue: date.Format("2006-01-02")}}, nil
	case *tree.DInterval:
		return &changefeedpb.Value{Value: &changefeedpb.Value_IntervalValue{IntervalValue: v.Duration.String()}}, nil
	case *tree.DUuid:
		return &changefeedpb.Value{Value: &changefeedpb.Value_UuidValue{UuidValue: strings.Trim(v.UUID.String(), "'")}}, nil
	case *tree.DTime, *tree.DTimeTZ:
		return &changefeedpb.Value{Value: &changefeedpb.Value_TimeValue{TimeValue: tree.AsStringWithFlags(v, tree.FmtBareStrings)}}, nil
	case *tree.DOid, *tree.DIPAddr, *tree.DBitArray, *tree.DBox2D,
		*tree.DTSVector, *tree.DTSQuery, *tree.DPGLSN, *tree.DPGVector:
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: d.String()}}, nil
	default:
		return nil, errors.AssertionFailedf("unexpected type %T for datumToProtoValue", d)
	}
}

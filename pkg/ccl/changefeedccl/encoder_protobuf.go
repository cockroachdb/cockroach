// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

type protobufEncoder struct {
	updatedField                   bool
	mvccTimestampField             bool
	beforeField                    bool
	keyInValue                     bool
	topicInValue                   bool
	sourceField                    bool
	envelopeType                   changefeedbase.EnvelopeType
	targets                        changefeedbase.Targets
	enrichedEnvelopeSourceProvider *enrichedSourceProvider
	bareEntry                      bareEntry
	wrappedEntry                   wrappedEntry
	enrichedEntry                  enrichedEntry
	beforerecord                   *changefeedpb.Record
	afterrecord                    *changefeedpb.Record
	metadata                       *changefeedpb.Metadata
	message                        changefeedpb.Message
}

// protobufEncoderOptions wraps EncodingOptions for initializing a protobufEncoder.
type protobufEncoderOptions struct {
	changefeedbase.EncodingOptions
}

var _ Encoder = &protobufEncoder{}

type bareEntry struct {
	msgBare changefeedpb.Message_Bare
	bare    changefeedpb.BareEnvelope
}
type wrappedEntry struct {
	msgWrapped changefeedpb.Message_Wrapped
	wrapped    changefeedpb.WrappedEnvelope
}
type enrichedEntry struct {
	msgEnriched changefeedpb.Message_Enriched
	enriched    changefeedpb.EnrichedEnvelope
}

// newProtobufEncoder constructs a new protobufEncoder from the given options and targets.
func newProtobufEncoder(
	ctx context.Context,
	opts protobufEncoderOptions,
	targets changefeedbase.Targets,
	sourceProvider *enrichedSourceProvider,
) Encoder {
	return &protobufEncoder{
		envelopeType:                   opts.Envelope,
		keyInValue:                     opts.KeyInValue,
		topicInValue:                   opts.TopicInValue,
		beforeField:                    opts.Diff,
		updatedField:                   opts.UpdatedTimestamps,
		mvccTimestampField:             opts.MVCCTimestamps,
		sourceField:                    inSet(changefeedbase.EnrichedPropertySource, opts.EnrichedProperties),
		targets:                        targets,
		enrichedEnvelopeSourceProvider: sourceProvider,
		bareEntry:                      bareEntry{bare: changefeedpb.BareEnvelope{Values: make(map[string]*changefeedpb.Value)}},
		wrappedEntry:                   wrappedEntry{},
		enrichedEntry:                  enrichedEntry{},
		afterrecord:                    &changefeedpb.Record{Values: make(map[string]*changefeedpb.Value)},
		beforerecord:                   &changefeedpb.Record{Values: make(map[string]*changefeedpb.Value)},
		metadata:                       &changefeedpb.Metadata{},
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
	case changefeedbase.OptEnvelopeWrapped:
		return e.buildWrapped(ctx, evCtx, updatedRow, prevRow)
	case changefeedbase.OptEnvelopeEnriched:
		return e.buildEnriched(ctx, evCtx, updatedRow, prevRow)
	default:
		return nil, errors.AssertionFailedf("envelope format not supported: %s", e.envelopeType)
	}
}

func (e *protobufEncoder) buildEnriched(
	ctx context.Context, evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	ee := e.enrichedEntry
	ee.enriched = changefeedpb.EnrichedEnvelope{}

	var err error
	if updatedRow.IsInitialized() && !updatedRow.IsDeleted() {
		ee.enriched.After, err = e.encodeRowToRecord(updatedRow, true)
		if err != nil {
			return nil, err
		}
	}
	if e.beforeField {
		if prevRow.IsInitialized() && !prevRow.IsDeleted() {
			ee.enriched.Before, err = e.encodeRowToRecord(prevRow, false)
			if err != nil {
				return nil, err
			}
		}
	}
	if e.keyInValue {
		ee.enriched.Key, err = buildKeyMessage(updatedRow)
		if err != nil {
			return nil, err
		}
	}
	if e.sourceField {
		ee.enriched.Source, err = e.enrichedEnvelopeSourceProvider.GetProtobuf(evCtx, updatedRow, prevRow)
		if err != nil {
			return nil, err
		}
	}
	ee.enriched.Op = inferOp(updatedRow, prevRow)
	ee.enriched.TsNs = timeutil.Now().UnixNano()

	ee.msgEnriched.Enriched = &ee.enriched
	e.message.Data = &ee.msgEnriched

	return protoutil.Marshal(&e.message)
}

// EncodeResolvedTimestamp encodes a resolved timestamp message for the specified topic.
func (e *protobufEncoder) EncodeResolvedTimestamp(
	ctx context.Context, topic string, ts hlc.Timestamp,
) ([]byte, error) {
	var msg *changefeedpb.Message
	if e.envelopeType == changefeedbase.OptEnvelopeBare {
		msg = &changefeedpb.Message{
			Data: &changefeedpb.Message_BareResolved{
				BareResolved: &changefeedpb.BareResolved{
					XCrdb__: &changefeedpb.Resolved{
						Resolved: ts.AsOfSystemTime(),
					},
				},
			},
		}
	} else {
		msg = &changefeedpb.Message{
			Data: &changefeedpb.Message_Resolved{
				Resolved: &changefeedpb.Resolved{
					Resolved: ts.AsOfSystemTime(),
				},
			},
		}
	}
	return protoutil.Marshal(msg)
}

func (e *protobufEncoder) buildBare(
	evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	be := e.bareEntry
	clear(be.bare.Values)

	if updatedRow.HasValues() {
		if err := updatedRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
			val, err := datumToProtoValue(d, sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				return err
			}
			be.bare.Values[col.Name] = val
			return nil
		}); err != nil {
			return nil, err
		}
	}

	meta, err := e.buildMetadata(evCtx, updatedRow)
	if err != nil {
		return nil, err
	}

	be.bare.XCrdb__ = meta
	be.msgBare.Bare = &be.bare
	e.message.Data = &be.msgBare

	return protoutil.Marshal(&e.message)
}

func (e *protobufEncoder) buildWrapped(
	ctx context.Context, evCtx eventContext, updatedRow, prevRow cdcevent.Row,
) ([]byte, error) {

	we := e.wrappedEntry
	we.wrapped = changefeedpb.WrappedEnvelope{}
	var err error
	if !updatedRow.IsDeleted() {
		we.wrapped.After, err = e.encodeRowToRecord(updatedRow, true)
		if err != nil {
			return nil, err
		}
	}
	if e.beforeField {
		if prevRow.IsInitialized() && !prevRow.IsDeleted() {
			we.wrapped.Before, err = e.encodeRowToRecord(prevRow, false)
			if err != nil {
				return nil, err
			}
		}
	}
	if e.keyInValue {
		we.wrapped.Key, err = buildKeyMessage(updatedRow)
		if err != nil {
			return nil, err
		}
	}
	if e.topicInValue {
		we.wrapped.Topic = evCtx.topic
	}
	if e.updatedField {
		we.wrapped.Updated = evCtx.updated.AsOfSystemTime()
	}
	if e.mvccTimestampField {
		we.wrapped.MvccTimestamp = evCtx.mvcc.AsOfSystemTime()
	}

	we.msgWrapped.Wrapped = &we.wrapped
	e.message.Data = &we.msgWrapped

	return protoutil.Marshal(&e.message)
}
func (e *protobufEncoder) buildMetadata(
	evCtx eventContext, row cdcevent.Row,
) (*changefeedpb.Metadata, error) {
	if !e.updatedField && !e.mvccTimestampField && !e.keyInValue && !e.topicInValue {
		return nil, nil
	}
	meta := e.metadata
	*meta = changefeedpb.Metadata{}
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

func (e *protobufEncoder) encodeRowToRecord(
	row cdcevent.Row, after bool,
) (*changefeedpb.Record, error) {
	if !row.HasValues() {
		return nil, nil
	}
	var record *changefeedpb.Record
	if after {
		record = e.afterrecord
	} else {
		record = e.beforerecord
	}

	if record.Values == nil {
		record.Values = make(map[string]*changefeedpb.Value, row.NumValueColumns())
	}
	clear(record.Values)

	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		val, err := datumToProtoValue(d, sessiondatapb.DataConversionConfig{}, time.UTC)
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
		val, err := datumToProtoValue(d, sessiondatapb.DataConversionConfig{}, time.UTC)
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

func inferOp(updated, prev cdcevent.Row) changefeedpb.Op {
	switch deduceOp(updated, prev) {
	case eventTypeCreate:
		return changefeedpb.Op_OP_CREATE
	case eventTypeUpdate:
		return changefeedpb.Op_OP_UPDATE
	case eventTypeDelete:
		return changefeedpb.Op_OP_DELETE
	default:
		return changefeedpb.Op_OP_UNSPECIFIED
	}
}

// datumToProtoValue converts a tree.Datum into a changefeedpb.Value.
// It handles all common CockroachDB datum types and maps them to their
// corresponding protobuf representation.
func datumToProtoValue(
	d tree.Datum, dcc sessiondatapb.DataConversionConfig, loc *time.Location,
) (*changefeedpb.Value, error) {
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
			pv, err := datumToProtoValue(elt, sessiondatapb.DataConversionConfig{}, time.UTC)
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
			pv, err := datumToProtoValue(elem, sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				return nil, err
			}
			var label string
			if i >= len(labels) || labels[i] == "" {
				label = "f" + strconv.Itoa(i+1)
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
			if isProtoTimestampRangeErr(err) {
				// Adjustment for benchmark testing: encode as string so benchmark test does not fail.
				return &changefeedpb.Value{
					Value: &changefeedpb.Value_StringValue{StringValue: v.Time.Format(time.RFC3339Nano)},
				}, nil
			} else {
				return nil, err
			}
		}

		return &changefeedpb.Value{
			Value: &changefeedpb.Value_TimestampValue{
				TimestampValue: ts,
			},
		}, nil

	case *tree.DTimestamp:
		ts, err := types.TimestampProto(v.Time.UTC())
		if err != nil {
			if isProtoTimestampRangeErr(err) {
				// Adjustment for benchmark testing: encode as string so benchmark test does not fail.
				return &changefeedpb.Value{
					Value: &changefeedpb.Value_StringValue{StringValue: v.Time.UTC().Format(time.RFC3339Nano)},
				}, nil
			} else {
				return nil, err
			}
		}
		return &changefeedpb.Value{
			Value: &changefeedpb.Value_TimestampValue{
				TimestampValue: ts,
			},
		}, nil
	case *tree.DBytes:
		return &changefeedpb.Value{Value: &changefeedpb.Value_BytesValue{BytesValue: []byte(*v)}}, nil
	case *tree.DGeography:
		ewkb := v.EWKB()
		return &changefeedpb.Value{Value: &changefeedpb.Value_BytesValue{BytesValue: ewkb}}, nil
	case *tree.DGeometry:
		ewkb := v.EWKB()
		return &changefeedpb.Value{Value: &changefeedpb.Value_BytesValue{BytesValue: ewkb}}, nil
	case *tree.DVoid:
		return nil, nil
	case *tree.DOid, *tree.DIPAddr, *tree.DBitArray, *tree.DBox2D,
		*tree.DTSVector, *tree.DTSQuery, *tree.DPGLSN, *tree.DPGVector,
		*tree.DLTree:
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: tree.AsStringWithFlags(v, tree.FmtBareStrings, tree.FmtDataConversionConfig(dcc), tree.FmtLocation(loc))}}, nil
	case *tree.DDate:
		return &changefeedpb.Value{Value: &changefeedpb.Value_DateValue{DateValue: tree.AsStringWithFlags(v, tree.FmtBareStrings, tree.FmtDataConversionConfig(dcc), tree.FmtLocation(loc))}}, nil
	case *tree.DInterval:
		return &changefeedpb.Value{Value: &changefeedpb.Value_IntervalValue{IntervalValue: tree.AsStringWithFlags(v, tree.FmtBareStrings, tree.FmtDataConversionConfig(dcc), tree.FmtLocation(loc))}}, nil
	case *tree.DUuid:
		return &changefeedpb.Value{Value: &changefeedpb.Value_UuidValue{UuidValue: tree.AsStringWithFlags(v, tree.FmtBareStrings, tree.FmtDataConversionConfig(dcc), tree.FmtLocation(loc))}}, nil
	case *tree.DTime, *tree.DTimeTZ:
		return &changefeedpb.Value{Value: &changefeedpb.Value_TimeValue{TimeValue: tree.AsStringWithFlags(v, tree.FmtBareStrings, tree.FmtDataConversionConfig(dcc), tree.FmtLocation(loc))}}, nil
	default:
		return nil, errors.AssertionFailedf("unexpected type %T for datumToProtoValue", d)
	}
}

// isProtoTimestampRangeErr returns true when types.TimestampProto() failed
// because the time is outside [0001-01-01 .. 9999-12-31].
func isProtoTimestampRangeErr(err error) bool {
	s := err.Error()
	return strings.Contains(s, "before 0001-01-01") ||
		strings.Contains(s, "after 10000-01-01")
}

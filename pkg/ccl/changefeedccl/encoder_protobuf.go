// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

type protobufEncoder struct {
	updatedField       bool                        // include logical update timestamp - timestamp when change occured
	mvccTimestampField bool                        // mvcc timestamp - exact commit time of change
	beforeField        bool                        // row before the update
	keyInValue         bool                        // if key is in the value field
	topicInValue       bool                        // kafka topic name in value
	envelopeType       changefeedbase.EnvelopeType // wrapped, enriched, bare
	targets            changefeedbase.Targets      // targeted tables
}

// options to initialize protobuf encoder
type protobufEncoderOptions struct {
	changefeedbase.EncodingOptions // options
}

var _ Encoder = &protobufEncoder{}

// construtor
func newProtobufEncoder(
	ctx context.Context, opts protobufEncoderOptions, targets changefeedbase.Targets,
) (Encoder, error) {
	log.Infof(ctx,
		"newProtobufEncoder: envelope=%s keyInValue=%v topicInValue=%v before=%v updated=%v mvcc=%v",
		opts.Envelope, opts.KeyInValue, opts.TopicInValue, opts.Diff, opts.UpdatedTimestamps, opts.MVCCTimestamps,
	)
	return &protobufEncoder{
		envelopeType:       opts.Envelope,
		keyInValue:         opts.KeyInValue,
		topicInValue:       opts.TopicInValue,
		beforeField:        opts.Diff,
		updatedField:       opts.UpdatedTimestamps,
		mvccTimestampField: opts.MVCCTimestamps,
		targets:            targets,
	}, nil
}

func (e *protobufEncoder) EncodeKey(ctx context.Context, row cdcevent.Row) ([]byte, error) {
	log.Infof(ctx, "EncodeKey")
	// Build a bare record of PK columns only:

	keyValues := make([]*changefeedpb.Value, 0)

	// Iterate over primary key columns and convert each datum to a Value.
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		keyValues = append(keyValues, datumToProtoValue(d))
		return nil
	}); err != nil {
		return nil, err
	}

	keyMsg := &changefeedpb.Key{
		Key: keyValues,
	}

	return proto.Marshal(keyMsg)
}

func (e *protobufEncoder) EncodeValue(
	ctx context.Context, evCtx eventContext, updatedRow, prevRow cdcevent.Row,
) ([]byte, error) {
	log.Infof(ctx, "EncodeValue (envelope=%s)", e.envelopeType)

	switch e.envelopeType {
	case changefeedbase.OptEnvelopeWrapped:
		return e.buildWrapped(ctx, evCtx, updatedRow, prevRow)

	case changefeedbase.OptEnvelopeBare:
		return e.buildBare(ctx, evCtx, updatedRow, prevRow)
	// case changefeedbase.OptEnvelopeEnriched:

	default:
		return nil, errors.Errorf("only wrapped envelopes supported, got %s", e.envelopeType)
	}
}

func (e *protobufEncoder) buildBare(
	ctx context.Context, evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {

	log.Infof(ctx, "In build Bare")

	/// Construct the main Values map from the updated row
	after := encodeRowToRecord(updatedRow)

	// Construct metadata if any flags are set
	var meta *changefeedpb.Metadata
	if e.updatedField || e.mvccTimestampField || e.keyInValue || e.topicInValue {
		meta = &changefeedpb.Metadata{}
		if e.updatedField {
			meta.Updated = evCtx.updated.AsOfSystemTime()
		}
		if e.mvccTimestampField {
			meta.MvccTimestamp = evCtx.mvcc.AsOfSystemTime()
		}
		if e.keyInValue {
			meta.Key = buildKeyMessage(updatedRow)
		}
		if e.topicInValue {
			meta.Topic = evCtx.topic
		}
	}

	// Create the bare envelope message
	env := &changefeedpb.Message{
		Data: &changefeedpb.Message_Bare{
			Bare: &changefeedpb.BareEnvelope{
				Values:   after.Values,
				Metadata: meta,
			},
		},
	}

	return proto.Marshal(env)
}

func (e *protobufEncoder) buildWrapped(
	ctx context.Context, evCtx eventContext, updatedRow, prevRow cdcevent.Row,
) ([]byte, error) {

	log.Infof(ctx, "In build wrapped")
	// do this for all - default
	after := encodeRowToRecord(updatedRow)

	// if before flag:
	var before *changefeedpb.Record
	if e.beforeField {
		before = encodeRowToRecord(prevRow)
	}

	// Optionally encode the key
	var keyMsg *changefeedpb.Key
	if e.keyInValue {
		keyMsg = buildKeyMessage(updatedRow)
	}

	// if topic in value flag
	var topicStr string
	if e.topicInValue {
		topicStr = evCtx.topic
	}

	// if timestamp flags
	var updatedStr, mvccStr string
	if e.updatedField {
		updatedStr = evCtx.updated.AsOfSystemTime()
	}
	if e.mvccTimestampField {
		mvccStr = evCtx.mvcc.AsOfSystemTime()
	}

	// build the wrapped envelope
	wrapped := &changefeedpb.WrappedEnvelope{
		After:         after,
		Before:        before,
		Key:           keyMsg,
		Topic:         topicStr,
		Updated:       updatedStr,
		MvccTimestamp: mvccStr,
	}

	// wrap in message
	env := &changefeedpb.Message{
		Data: &changefeedpb.Message_Wrapped{Wrapped: wrapped},
	}
	// serialize
	return proto.Marshal(env)
}

func encodeRowToRecord(row cdcevent.Row) *changefeedpb.Record {
	rec := &changefeedpb.Record{Values: make(map[string]*changefeedpb.Value)}
	_ = row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		rec.Values[col.Name] = datumToProtoValue(d)
		return nil
	})
	return rec
}

func buildKeyMessage(row cdcevent.Row) *changefeedpb.Key {
	keyValues := make([]*changefeedpb.Value, 0)
	_ = row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		keyValues = append(keyValues, datumToProtoValue(d))
		return nil
	})
	return &changefeedpb.Key{Key: keyValues}
}

func datumToProtoValue(d tree.Datum) *changefeedpb.Value {
	if d == tree.DNull {
		// null values?
		return &changefeedpb.Value{}
	}
	switch v := d.(type) {
	case *tree.DInt:
		return &changefeedpb.Value{Value: &changefeedpb.Value_Int64Value{Int64Value: int64(*v)}}
	case *tree.DString:
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: string(*v)}}
	case *tree.DBool:
		return &changefeedpb.Value{Value: &changefeedpb.Value_BoolValue{BoolValue: bool(*v)}}
	case *tree.DFloat:
		return &changefeedpb.Value{Value: &changefeedpb.Value_DoubleValue{DoubleValue: float64(*v)}}

	// case *tree.DTimestamp, *tree.DTimestampTZ:
	// 	t := tree.MustBeDDateTime(v).Time
	// 	ts := timestamppb.New(t)
	// 	return &changefeedpb.Value{Value: &changefeedpb.Value_TimestampValue{TimestampValue: ts}}

	case *tree.DDecimal:
		f, err := v.Float64()
		if err == nil {
			return &changefeedpb.Value{Value: &changefeedpb.Value_DoubleValue{DoubleValue: f}}
		}
		// clean this part up?
		// If conversion failed, fall back to string
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: v.String()}}

	default:
		// fallback to string
		return &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: d.String()}}
	}
}

func (e *protobufEncoder) EncodeResolvedTimestamp(
	ctx context.Context, topic string, ts hlc.Timestamp,
) ([]byte, error) {

	log.Infof(ctx, "EncodeResolvedTimestamp: %s @ %s", topic, ts.AsOfSystemTime())
	resolvedValue := &changefeedpb.Value{
		Value: &changefeedpb.Value_StringValue{
			StringValue: ts.AsOfSystemTime(),
		},
	}

	msg := &changefeedpb.Message{
		Data: &changefeedpb.Message_Bare{
			Bare: &changefeedpb.BareEnvelope{
				Values: map[string]*changefeedpb.Value{
					"resolved": resolvedValue,
				},
				Metadata: &changefeedpb.Metadata{
					Topic: topic,
				},
			},
		},
	}

	return proto.Marshal(msg)
}

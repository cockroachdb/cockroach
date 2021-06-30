// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"encoding/binary"
	gojson "encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	confluentSubjectSuffixKey   = `-key`
	confluentSubjectSuffixValue = `-value`
)

// encodeRow holds all the pieces necessary to encode a row change into a key or
// value.
type encodeRow struct {
	// datums is the new value of a changed table row.
	datums rowenc.EncDatumRow
	// updated is the mvcc timestamp corresponding to the latest
	// update in `datums` or, if the row is part of a backfill,
	// the time at which the backfill was started.
	updated hlc.Timestamp
	// mvccTimestamp is the mvcc timestamp corresponding to the
	// latest update in `datums`.
	mvccTimestamp hlc.Timestamp
	// deleted is true if row is a deletion. In this case, only the primary
	// key columns are guaranteed to be set in `datums`.
	deleted bool
	// tableDesc is a TableDescriptor for the table containing `datums`.
	// It's valid for interpreting the row at `updated`.
	tableDesc catalog.TableDescriptor
	// prevDatums is the old value of a changed table row. The field is set
	// to nil if the before value for changes was not requested (OptDiff).
	prevDatums rowenc.EncDatumRow
	// prevDeleted is true if prevDatums is missing or is a deletion.
	prevDeleted bool
	// prevTableDesc is a TableDescriptor for the table containing `prevDatums`.
	// It's valid for interpreting the row at `updated.Prev()`.
	prevTableDesc catalog.TableDescriptor
}

// Encoder turns a row into a serialized changefeed key, value, or resolved
// timestamp. It represents one of the `format=` changefeed options.
type Encoder interface {
	// EncodeKey encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`, but only the primary key fields will be used. The
	// returned bytes are only valid until the next call to Encode*.
	EncodeKey(context.Context, encodeRow) ([]byte, error)
	// EncodeValue encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeValue(context.Context, encodeRow) ([]byte, error)
	// EncodeResolvedTimestamp encodes a resolved timestamp payload for the
	// given topic name. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeResolvedTimestamp(context.Context, string, hlc.Timestamp) ([]byte, error)
}

func getEncoder(
	ctx context.Context, opts map[string]string, targets jobspb.ChangefeedTargets,
) (Encoder, error) {
	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case ``, changefeedbase.OptFormatJSON:
		return makeJSONEncoder(opts, targets)
	case changefeedbase.OptFormatAvro:
		return newConfluentAvroEncoder(ctx, opts, targets)
	case changefeedbase.OptFormatNative:
		return &nativeEncoder{}, nil
	default:
		return nil, errors.Errorf(`unknown %s: %s`, changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}
}

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	updatedField, mvccTimestampField, beforeField, wrapped, keyOnly, keyInValue, topicInValue bool

	targets jobspb.ChangefeedTargets
	alloc   rowenc.DatumAlloc
	buf     bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(
	opts map[string]string, targets jobspb.ChangefeedTargets,
) (*jsonEncoder, error) {
	e := &jsonEncoder{
		targets: targets,
		keyOnly: changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeKeyOnly,
		wrapped: changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeWrapped,
	}
	_, e.updatedField = opts[changefeedbase.OptUpdatedTimestamps]
	_, e.mvccTimestampField = opts[changefeedbase.OptMVCCTimestamps]
	_, e.beforeField = opts[changefeedbase.OptDiff]
	if e.beforeField && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptDiff, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	_, e.keyInValue = opts[changefeedbase.OptKeyInValue]
	if e.keyInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptKeyInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	_, e.topicInValue = opts[changefeedbase.OptTopicInValue]
	if e.topicInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptTopicInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(_ context.Context, row encodeRow) ([]byte, error) {
	jsonEntries, err := e.encodeKeyRaw(row)
	if err != nil {
		return nil, err
	}
	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

func (e *jsonEncoder) encodeKeyRaw(row encodeRow) ([]interface{}, error) {
	colIdxByID := catalog.ColumnIDToOrdinalMap(row.tableDesc.PublicColumns())
	primaryIndex := row.tableDesc.GetPrimaryIndex()
	jsonEntries := make([]interface{}, primaryIndex.NumKeyColumns())
	for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
		colID := primaryIndex.GetKeyColumnID(i)
		idx, ok := colIdxByID.Get(colID)
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum := row.datums[idx]
		datum, col := row.datums[idx], row.tableDesc.PublicColumns()[idx]
		if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(
			datum.Datum,
			sessiondatapb.DataConversionConfig{},
			time.UTC,
		)
		if err != nil {
			return nil, err
		}
	}
	return jsonEntries, nil
}

func (e *jsonEncoder) encodeTopicRaw(row encodeRow) (interface{}, error) {
	descID := row.tableDesc.GetID()
	// use the target list since row.tableDesc.GetName() will not have fully qualified names
	topicName, ok := e.targets[descID]
	if !ok {
		return nil, fmt.Errorf("table with name %s and descriptor ID %d not found in changefeed target list",
			row.tableDesc.GetName(), descID)
	}
	return topicName.StatementTimeName, nil
}

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(_ context.Context, row encodeRow) ([]byte, error) {
	if e.keyOnly || (!e.wrapped && row.deleted) {
		return nil, nil
	}

	var after map[string]interface{}
	if !row.deleted {
		columns := row.tableDesc.PublicColumns()
		after = make(map[string]interface{}, len(columns))
		for i, col := range columns {
			datum := row.datums[i]
			if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
				return nil, err
			}
			var err error
			after[col.GetName()], err = tree.AsJSON(
				datum.Datum,
				sessiondatapb.DataConversionConfig{},
				time.UTC,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	var before map[string]interface{}
	if row.prevDatums != nil && !row.prevDeleted {
		columns := row.prevTableDesc.PublicColumns()
		before = make(map[string]interface{}, len(columns))
		for i, col := range columns {
			datum := row.prevDatums[i]
			if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
				return nil, err
			}
			var err error
			before[col.GetName()], err = tree.AsJSON(
				datum.Datum,
				sessiondatapb.DataConversionConfig{},
				time.UTC,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	var jsonEntries map[string]interface{}
	if e.wrapped {
		if after != nil {
			jsonEntries = map[string]interface{}{`after`: after}
		} else {
			jsonEntries = map[string]interface{}{`after`: nil}
		}
		if e.beforeField {
			if before != nil {
				jsonEntries[`before`] = before
			} else {
				jsonEntries[`before`] = nil
			}
		}
		if e.keyInValue {
			keyEntries, err := e.encodeKeyRaw(row)
			if err != nil {
				return nil, err
			}
			jsonEntries[`key`] = keyEntries
		}
		if e.topicInValue {
			topicEntry, err := e.encodeTopicRaw(row)
			if err != nil {
				return nil, err
			}
			jsonEntries[`topic`] = topicEntry
		}
	} else {
		jsonEntries = after
	}

	if e.updatedField || e.mvccTimestampField {
		var meta map[string]interface{}
		if e.wrapped {
			meta = jsonEntries
		} else {
			meta = make(map[string]interface{}, 1)
			jsonEntries[jsonMetaSentinel] = meta
		}
		if e.updatedField {
			meta[`updated`] = row.updated.AsOfSystemTime()
		}
		if e.mvccTimestampField {
			meta[`mvcc_timestamp`] = row.mvccTimestamp.AsOfSystemTime()
		}
	}

	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *jsonEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, resolved hlc.Timestamp,
) ([]byte, error) {
	meta := map[string]interface{}{
		`resolved`: tree.TimestampToDecimalDatum(resolved).Decimal.String(),
	}
	var jsonEntries interface{}
	if e.wrapped {
		jsonEntries = meta
	} else {
		jsonEntries = map[string]interface{}{
			jsonMetaSentinel: meta,
		}
	}
	return gojson.Marshal(jsonEntries)
}

// confluentAvroEncoder encodes changefeed entries as Avro's binary or textual
// JSON format. Keys are the primary key columns in a record. Values are all
// columns in a record.
type confluentAvroEncoder struct {
	schemaRegistry                     schemaRegistry
	schemaPrefix                       string
	updatedField, beforeField, keyOnly bool
	targets                            jobspb.ChangefeedTargets

	keyCache      map[tableIDAndVersion]confluentRegisteredKeySchema
	valueCache    map[tableIDAndVersionPair]confluentRegisteredEnvelopeSchema
	resolvedCache map[string]confluentRegisteredEnvelopeSchema
}

type tableIDAndVersion uint64
type tableIDAndVersionPair [2]tableIDAndVersion // [before, after]

func makeTableIDAndVersion(id descpb.ID, version descpb.DescriptorVersion) tableIDAndVersion {
	return tableIDAndVersion(id)<<32 + tableIDAndVersion(version)
}

type confluentRegisteredKeySchema struct {
	schema     *avroDataRecord
	registryID int32
}

type confluentRegisteredEnvelopeSchema struct {
	schema     *avroEnvelopeRecord
	registryID int32
}

var _ Encoder = &confluentAvroEncoder{}

func newConfluentAvroEncoder(
	ctx context.Context, opts map[string]string, targets jobspb.ChangefeedTargets,
) (*confluentAvroEncoder, error) {
	e := &confluentAvroEncoder{
		schemaPrefix: opts[changefeedbase.OptAvroSchemaPrefix],
		targets:      targets,
	}

	switch opts[changefeedbase.OptEnvelope] {
	case string(changefeedbase.OptEnvelopeKeyOnly):
		e.keyOnly = true
	case string(changefeedbase.OptEnvelopeWrapped):
	default:
		return nil, errors.Errorf(`%s=%s is not supported with %s=%s`,
			changefeedbase.OptEnvelope, opts[changefeedbase.OptEnvelope], changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}
	_, e.updatedField = opts[changefeedbase.OptUpdatedTimestamps]
	if e.updatedField && e.keyOnly {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptUpdatedTimestamps, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	_, e.beforeField = opts[changefeedbase.OptDiff]
	if e.beforeField && e.keyOnly {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptDiff, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}

	if _, ok := opts[changefeedbase.OptKeyInValue]; ok {
		return nil, errors.Errorf(`%s is not supported with %s=%s`,
			changefeedbase.OptKeyInValue, changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}
	if _, ok := opts[changefeedbase.OptTopicInValue]; ok {
		return nil, errors.Errorf(`%s is not supported with %s=%s`,
			changefeedbase.OptTopicInValue, changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}
	if len(opts[changefeedbase.OptConfluentSchemaRegistry]) == 0 {
		return nil, errors.Errorf(`WITH option %s is required for %s=%s`,
			changefeedbase.OptConfluentSchemaRegistry, changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}

	reg, err := newConfluentSchemaRegistry(opts[changefeedbase.OptConfluentSchemaRegistry])
	if err != nil {
		return nil, err
	}
	e.schemaRegistry = reg

	if err := reg.Ping(ctx); err != nil {
		return nil, errors.Wrap(err, "schema registry unavailable")
	}

	e.keyCache = make(map[tableIDAndVersion]confluentRegisteredKeySchema)
	e.valueCache = make(map[tableIDAndVersionPair]confluentRegisteredEnvelopeSchema)
	e.resolvedCache = make(map[string]confluentRegisteredEnvelopeSchema)
	return e, nil
}

// Get the raw SQL-formatted string for a table name
// and apply full_table_name and avro_schema_prefix options
func (e *confluentAvroEncoder) rawTableName(desc catalog.TableDescriptor) string {
	return e.schemaPrefix + e.targets[desc.GetID()].StatementTimeName
}

// EncodeKey implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeKey(ctx context.Context, row encodeRow) ([]byte, error) {
	cacheKey := makeTableIDAndVersion(row.tableDesc.GetID(), row.tableDesc.GetVersion())

	registered, ok := e.keyCache[cacheKey]
	if !ok {
		var err error
		tableName := e.rawTableName(row.tableDesc)
		registered.schema, err = indexToAvroSchema(row.tableDesc, row.tableDesc.GetPrimaryIndex(), tableName, e.schemaPrefix)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(tableName) + confluentSubjectSuffixKey
		registered.registryID, err = e.register(ctx, &registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.keyCache[cacheKey] = registered
	}

	if ok {
		registered.schema.refreshTypeMetadata(row.tableDesc)
	}

	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		changefeedbase.ConfluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, row.datums)
}

// EncodeValue implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeValue(ctx context.Context, row encodeRow) ([]byte, error) {
	if e.keyOnly {
		return nil, nil
	}

	var cacheKey tableIDAndVersionPair
	if e.beforeField && row.prevTableDesc != nil {
		cacheKey[0] = makeTableIDAndVersion(row.prevTableDesc.GetID(), row.prevTableDesc.GetVersion())
	}
	cacheKey[1] = makeTableIDAndVersion(row.tableDesc.GetID(), row.tableDesc.GetVersion())
	registered, ok := e.valueCache[cacheKey]
	if !ok {
		var beforeDataSchema *avroDataRecord
		if e.beforeField && row.prevTableDesc != nil {
			var err error
			beforeDataSchema, err = tableToAvroSchema(row.prevTableDesc, `before`, e.schemaPrefix)
			if err != nil {
				return nil, err
			}
		}

		afterDataSchema, err := tableToAvroSchema(row.tableDesc, avroSchemaNoSuffix, e.schemaPrefix)
		if err != nil {
			return nil, err
		}

		opts := avroEnvelopeOpts{afterField: true, beforeField: e.beforeField, updatedField: e.updatedField}
		registered.schema, err = envelopeToAvroSchema(e.rawTableName(row.tableDesc), opts, beforeDataSchema, afterDataSchema, e.schemaPrefix)

		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(e.rawTableName(row.tableDesc)) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(ctx, &registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.valueCache[cacheKey] = registered
	}
	if ok {
		registered.schema.after.refreshTypeMetadata(row.tableDesc)
		if row.prevTableDesc != nil && registered.schema.before != nil {
			registered.schema.before.refreshTypeMetadata(row.prevTableDesc)
		}
	}

	var meta avroMetadata
	if registered.schema.opts.updatedField {
		meta = map[string]interface{}{
			`updated`: row.updated,
		}
	}
	var beforeDatums, afterDatums rowenc.EncDatumRow
	if row.prevDatums != nil && !row.prevDeleted {
		beforeDatums = row.prevDatums
	}
	if !row.deleted {
		afterDatums = row.datums
	}
	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		changefeedbase.ConfluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, beforeDatums, afterDatums)
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeResolvedTimestamp(
	ctx context.Context, topic string, resolved hlc.Timestamp,
) ([]byte, error) {
	registered, ok := e.resolvedCache[topic]
	if !ok {
		opts := avroEnvelopeOpts{resolvedField: true}
		var err error
		registered.schema, err = envelopeToAvroSchema(topic, opts, nil /* before */, nil /* after */, e.schemaPrefix /* namespace */)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(topic) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(ctx, &registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.resolvedCache[topic] = registered
	}
	var meta avroMetadata
	if registered.schema.opts.resolvedField {
		meta = map[string]interface{}{
			`resolved`: resolved,
		}
	}
	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		changefeedbase.ConfluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, nil /* beforeRow */, nil /* afterRow */)
}

func (e *confluentAvroEncoder) register(
	ctx context.Context, schema *avroRecord, subject string,
) (int32, error) {
	return e.schemaRegistry.RegisterSchemaForSubject(ctx, subject, schema.codec.Schema())
}

// nativeEncoder only implements EncodeResolvedTimestamp.
// Unfortunately, the encoder assumes that it operates with encodeRow -- something
// that's just not the case when emitting raw KVs.
// In addition, there is a kafka specific concept (topic) that's exposed at the Encoder level.
// TODO(yevgeniy): Refactor encoder interface so that it operates on kvfeed events.
// In addition, decouple the concept of topic from the Encoder.
type nativeEncoder struct{}

func (e *nativeEncoder) EncodeKey(ctx context.Context, row encodeRow) ([]byte, error) {
	panic("EncodeKey should not be called on nativeEncoder")
}

func (e *nativeEncoder) EncodeValue(ctx context.Context, row encodeRow) ([]byte, error) {
	panic("EncodeValue should not be called on nativeEncoder")
}

func (e *nativeEncoder) EncodeResolvedTimestamp(
	ctx context.Context, s string, ts hlc.Timestamp,
) ([]byte, error) {
	return protoutil.Marshal(&ts)
}

var _ Encoder = &nativeEncoder{}

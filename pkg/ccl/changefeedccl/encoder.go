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
	"io/ioutil"
	"net/url"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

const (
	confluentSchemaContentType   = `application/vnd.schemaregistry.v1+json`
	confluentSubjectSuffixKey    = `-key`
	confluentSubjectSuffixValue  = `-value`
	confluentAvroWireFormatMagic = byte(0)
)

// encodeRow holds all the pieces necessary to encode a row change into a key or
// value.
type encodeRow struct {
	// datums is the new value of a changed table row.
	datums sqlbase.EncDatumRow
	// updated is the mvcc timestamp corresponding to the latest update in
	// `datums`.
	updated hlc.Timestamp
	// deleted is true if row is a deletion. In this case, only the primary
	// key columns are guaranteed to be set in `datums`.
	deleted bool
	// tableDesc is a TableDescriptor for the table containing `datums`.
	// It's valid for interpreting the row at `updated`.
	tableDesc *sqlbase.TableDescriptor
	// prevDatums is the old value of a changed table row. The field is set
	// to nil if the before value for changes was not requested (OptDiff).
	prevDatums sqlbase.EncDatumRow
	// prevDeleted is true if prevDatums is missing or is a deletion.
	prevDeleted bool
	// prevTableDesc is a TableDescriptor for the table containing `prevDatums`.
	// It's valid for interpreting the row at `updated.Prev()`.
	prevTableDesc *sqlbase.TableDescriptor
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

func getEncoder(opts map[string]string) (Encoder, error) {
	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case ``, changefeedbase.OptFormatJSON:
		return makeJSONEncoder(opts)
	case changefeedbase.OptFormatAvro:
		return newConfluentAvroEncoder(opts)
	default:
		return nil, errors.Errorf(`unknown %s: %s`, changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}
}

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	updatedField, beforeField, wrapped, keyOnly, keyInValue bool

	alloc sqlbase.DatumAlloc
	buf   bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(opts map[string]string) (*jsonEncoder, error) {
	e := &jsonEncoder{
		keyOnly: changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeKeyOnly,
		wrapped: changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeWrapped,
	}
	_, e.updatedField = opts[changefeedbase.OptUpdatedTimestamps]
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
	colIdxByID := row.tableDesc.ColumnIdxMap()
	jsonEntries := make([]interface{}, len(row.tableDesc.PrimaryIndex.ColumnIDs))
	for i, colID := range row.tableDesc.PrimaryIndex.ColumnIDs {
		idx, ok := colIdxByID[colID]
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum, col := row.datums[idx], &row.tableDesc.Columns[idx]
		if err := datum.EnsureDecoded(col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(datum.Datum, time.UTC)
		if err != nil {
			return nil, err
		}
	}
	return jsonEntries, nil
}

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(_ context.Context, row encodeRow) ([]byte, error) {
	if e.keyOnly || (!e.wrapped && row.deleted) {
		return nil, nil
	}

	var after map[string]interface{}
	if !row.deleted {
		columns := row.tableDesc.Columns
		after = make(map[string]interface{}, len(columns))
		for i := range columns {
			col := &columns[i]
			datum := row.datums[i]
			if err := datum.EnsureDecoded(col.Type, &e.alloc); err != nil {
				return nil, err
			}
			var err error
			after[col.Name], err = tree.AsJSON(datum.Datum, time.UTC)
			if err != nil {
				return nil, err
			}
		}
	}

	var before map[string]interface{}
	if row.prevDatums != nil && !row.prevDeleted {
		columns := row.prevTableDesc.Columns
		before = make(map[string]interface{}, len(columns))
		for i := range columns {
			col := &columns[i]
			datum := row.prevDatums[i]
			if err := datum.EnsureDecoded(col.Type, &e.alloc); err != nil {
				return nil, err
			}
			var err error
			before[col.Name], err = tree.AsJSON(datum.Datum, time.UTC)
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
	} else {
		jsonEntries = after
	}

	if e.updatedField {
		var meta map[string]interface{}
		if e.wrapped {
			meta = jsonEntries
		} else {
			meta = make(map[string]interface{}, 1)
			jsonEntries[jsonMetaSentinel] = meta
		}
		meta[`updated`] = row.updated.AsOfSystemTime()
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
		`resolved`: tree.TimestampToDecimal(resolved).Decimal.String(),
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
	registryURL                        string
	updatedField, beforeField, keyOnly bool

	keyCache      map[tableIDAndVersion]confluentRegisteredKeySchema
	valueCache    map[tableIDAndVersionPair]confluentRegisteredEnvelopeSchema
	resolvedCache map[string]confluentRegisteredEnvelopeSchema
}

type tableIDAndVersion uint64
type tableIDAndVersionPair [2]tableIDAndVersion // [before, after]

func makeTableIDAndVersion(id sqlbase.ID, version sqlbase.DescriptorVersion) tableIDAndVersion {
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

func newConfluentAvroEncoder(opts map[string]string) (*confluentAvroEncoder, error) {
	e := &confluentAvroEncoder{registryURL: opts[changefeedbase.OptConfluentSchemaRegistry]}

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

	if len(e.registryURL) == 0 {
		return nil, errors.Errorf(`WITH option %s is required for %s=%s`,
			changefeedbase.OptConfluentSchemaRegistry, changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}

	e.keyCache = make(map[tableIDAndVersion]confluentRegisteredKeySchema)
	e.valueCache = make(map[tableIDAndVersionPair]confluentRegisteredEnvelopeSchema)
	e.resolvedCache = make(map[string]confluentRegisteredEnvelopeSchema)
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeKey(ctx context.Context, row encodeRow) ([]byte, error) {
	cacheKey := makeTableIDAndVersion(row.tableDesc.ID, row.tableDesc.Version)
	registered, ok := e.keyCache[cacheKey]
	if !ok {
		var err error
		registered.schema, err = indexToAvroSchema(row.tableDesc, &row.tableDesc.PrimaryIndex)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(row.tableDesc.Name) + confluentSubjectSuffixKey
		registered.registryID, err = e.register(ctx, &registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.keyCache[cacheKey] = registered
	}

	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		confluentAvroWireFormatMagic,
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
		cacheKey[0] = makeTableIDAndVersion(row.prevTableDesc.ID, row.prevTableDesc.Version)
	}
	cacheKey[1] = makeTableIDAndVersion(row.tableDesc.ID, row.tableDesc.Version)
	registered, ok := e.valueCache[cacheKey]
	if !ok {
		var beforeDataSchema *avroDataRecord
		if e.beforeField && row.prevTableDesc != nil {
			var err error
			beforeDataSchema, err = tableToAvroSchema(row.prevTableDesc, `before`)
			if err != nil {
				return nil, err
			}
		}

		afterDataSchema, err := tableToAvroSchema(row.tableDesc, avroSchemaNoSuffix)
		if err != nil {
			return nil, err
		}

		opts := avroEnvelopeOpts{afterField: true, beforeField: e.beforeField, updatedField: e.updatedField}
		registered.schema, err = envelopeToAvroSchema(row.tableDesc.Name, opts, beforeDataSchema, afterDataSchema)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(row.tableDesc.Name) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(ctx, &registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.valueCache[cacheKey] = registered
	}
	var meta avroMetadata
	if registered.schema.opts.updatedField {
		meta = map[string]interface{}{
			`updated`: row.updated,
		}
	}
	var beforeDatums, afterDatums sqlbase.EncDatumRow
	if row.prevDatums != nil && !row.prevDeleted {
		beforeDatums = row.prevDatums
	}
	if !row.deleted {
		afterDatums = row.datums
	}
	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		confluentAvroWireFormatMagic,
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
		registered.schema, err = envelopeToAvroSchema(topic, opts, nil /* before */, nil /* after */)
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
		confluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, nil /* beforeRow */, nil /* afterRow */)
}

func (e *confluentAvroEncoder) register(
	ctx context.Context, schema *avroRecord, subject string,
) (int32, error) {
	type confluentSchemaVersionRequest struct {
		Schema string `json:"schema"`
	}
	type confluentSchemaVersionResponse struct {
		ID int32 `json:"id"`
	}

	url, err := url.Parse(e.registryURL)
	if err != nil {
		return 0, err
	}
	url.Path = filepath.Join(url.EscapedPath(), `subjects`, subject, `versions`)

	schemaStr := schema.codec.Schema()
	if log.V(1) {
		log.Infof(ctx, "registering avro schema %s %s", url, schemaStr)
	}

	req := confluentSchemaVersionRequest{Schema: schemaStr}
	var buf bytes.Buffer
	if err := gojson.NewEncoder(&buf).Encode(req); err != nil {
		return 0, err
	}

	var id int32

	// Since network services are often a source of flakes, add a few retries here
	// before we give up and return an error that will bubble up and tear down the
	// entire changefeed, though that error is marked as retryable so that the job
	// itself can attempt to start the changefeed again. TODO(dt): If the registry
	// is down or constantly returning errors, we can't make progress. Continuing
	// to indicate that we're "running" in this case can be misleading, as we
	// really aren't anymore. Right now the MO in CDC is try and try again
	// forever, so doing so here is consistent with the behavior elsewhere, but we
	// should revisit this more broadly as this pattern can easily mask real,
	// actionable issues in the operator's environment that which they might be
	// able to resolve if we made them visible in a failure instead.
	if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), 3, func() error {
		resp, err := httputil.Post(ctx, url.String(), confluentSchemaContentType, &buf)
		if err != nil {
			return errors.Wrap(err, "contacting confluent schema registry")
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := ioutil.ReadAll(resp.Body)
			return errors.Errorf(`registering schema to %s %s: %s`, url.String(), resp.Status, body)
		}
		var res confluentSchemaVersionResponse
		if err := gojson.NewDecoder(resp.Body).Decode(&res); err != nil {
			return errors.Wrap(err, "decoding confluent schema registry reply")
		}
		id = res.ID
		return nil
	}); err != nil {
		log.Warningf(ctx, "%+v", err)
		return 0, MarkRetryableError(err)
	}

	return id, nil
}

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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
	switch formatType(opts[optFormat]) {
	case ``, optFormatJSON:
		return makeJSONEncoder(opts)
	case optFormatAvro:
		return newConfluentAvroEncoder(opts)
	default:
		return nil, errors.Errorf(`unknown %s: %s`, optFormat, opts[optFormat])
	}
}

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	updatedField, wrapped, keyOnly, keyInValue bool

	alloc sqlbase.DatumAlloc
	buf   bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(opts map[string]string) (*jsonEncoder, error) {
	e := &jsonEncoder{
		keyOnly: envelopeType(opts[optEnvelope]) == optEnvelopeKeyOnly,
		wrapped: envelopeType(opts[optEnvelope]) == optEnvelopeWrapped,
	}
	_, e.updatedField = opts[optUpdatedTimestamps]
	_, e.keyInValue = opts[optKeyInValue]
	if e.keyInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			optKeyInValue, optEnvelope, optEnvelopeWrapped)
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
		if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(datum.Datum)
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
			if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
				return nil, err
			}
			var err error
			after[col.Name], err = tree.AsJSON(datum.Datum)
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
	registryURL           string
	updatedField, keyOnly bool

	keyCache      map[tableIDAndVersion]confluentRegisteredKeySchema
	valueCache    map[tableIDAndVersion]confluentRegisteredEnvelopeSchema
	resolvedCache map[string]confluentRegisteredEnvelopeSchema
}

type tableIDAndVersion uint64

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
	e := &confluentAvroEncoder{registryURL: opts[optConfluentSchemaRegistry]}

	switch opts[optEnvelope] {
	case string(optEnvelopeKeyOnly):
		e.keyOnly = true
	case string(optEnvelopeWrapped):
	default:
		return nil, errors.Errorf(`%s=%s is not supported with %s=%s`,
			optEnvelope, opts[optEnvelope], optFormat, optFormatAvro)
	}
	_, e.updatedField = opts[optUpdatedTimestamps]

	if _, ok := opts[optKeyInValue]; ok {
		return nil, errors.Errorf(`%s is not supported with %s=%s`,
			optKeyInValue, optFormat, optFormatAvro)
	}

	if len(e.registryURL) == 0 {
		return nil, errors.Errorf(`WITH option %s is required for %s=%s`,
			optConfluentSchemaRegistry, optFormat, optFormatAvro)
	}

	e.keyCache = make(map[tableIDAndVersion]confluentRegisteredKeySchema)
	e.valueCache = make(map[tableIDAndVersion]confluentRegisteredEnvelopeSchema)
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

	cacheKey := makeTableIDAndVersion(row.tableDesc.ID, row.tableDesc.Version)
	registered, ok := e.valueCache[cacheKey]
	if !ok {
		afterDataSchema, err := tableToAvroSchema(row.tableDesc)
		if err != nil {
			return nil, err
		}

		opts := avroEnvelopeOpts{afterField: true, updatedField: e.updatedField}
		registered.schema, err = envelopeToAvroSchema(row.tableDesc.Name, opts, afterDataSchema)
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
	var datums sqlbase.EncDatumRow
	if !row.deleted {
		datums = row.datums
	}
	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		confluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, datums)
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeResolvedTimestamp(
	ctx context.Context, topic string, resolved hlc.Timestamp,
) ([]byte, error) {
	registered, ok := e.resolvedCache[topic]
	if !ok {
		opts := avroEnvelopeOpts{resolvedField: true}
		var err error
		registered.schema, err = envelopeToAvroSchema(topic, opts, nil /* after */)
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
	return registered.schema.BinaryFromRow(header, meta, nil /* row */)
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
		log.Infof(context.TODO(), "registering avro schema %s %s", url, schemaStr)
	}

	req := confluentSchemaVersionRequest{Schema: schemaStr}
	var buf bytes.Buffer
	if err := gojson.NewEncoder(&buf).Encode(req); err != nil {
		return 0, err
	}

	// TODO(someone): connect the context to the caller to obey
	// cancellation.
	resp, err := httputil.Post(ctx, url.String(), confluentSchemaContentType, &buf)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := ioutil.ReadAll(resp.Body)
		return 0, errors.Errorf(`registering schema to %s %s: %s`, url.String(), resp.Status, body)
	}
	var res confluentSchemaVersionResponse
	if err := gojson.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, err
	}

	return res.ID, nil
}

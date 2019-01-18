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
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// Encoder turns a row into a serialized changefeed key, value, or resolved
// timestamp. It represents one of the `format=` changefeed options.
type Encoder interface {
	// EncodeKey encodes the primary key of the given row. The columns of the
	// row are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`, but only the primary key fields will be used. The
	// returned bytes are only valid until the next call to Encode*.
	EncodeKey(*sqlbase.TableDescriptor, sqlbase.EncDatumRow) ([]byte, error)
	// EncodeValue encodes the primary key of the given row. The columns of the
	// row are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeValue(*sqlbase.TableDescriptor, sqlbase.EncDatumRow, hlc.Timestamp) ([]byte, error)
	// EncodeResolvedTimestamp encodes a resolved timestamp payload. The
	// returned bytes are only valid until the next call to Encode*.
	EncodeResolvedTimestamp(string, hlc.Timestamp) ([]byte, error)
}

func getEncoder(opts map[string]string) (Encoder, error) {
	switch formatType(opts[optFormat]) {
	case ``, optFormatJSON:
		return makeJSONEncoder(opts), nil
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
	opts map[string]string

	alloc sqlbase.DatumAlloc
	buf   bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(opts map[string]string) *jsonEncoder {
	return &jsonEncoder{opts: opts}
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(
	tableDesc *sqlbase.TableDescriptor, row sqlbase.EncDatumRow,
) ([]byte, error) {
	colIdxByID := tableDesc.ColumnIdxMap()
	jsonEntries := make([]interface{}, len(tableDesc.PrimaryIndex.ColumnIDs))
	for i, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		idx, ok := colIdxByID[colID]
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum, col := row[idx], tableDesc.Columns[idx]
		if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(datum.Datum)
		if err != nil {
			return nil, err
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

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(
	tableDesc *sqlbase.TableDescriptor, row sqlbase.EncDatumRow, updated hlc.Timestamp,
) ([]byte, error) {
	columns := tableDesc.Columns
	jsonEntries := make(map[string]interface{}, len(columns))
	if _, ok := e.opts[optUpdatedTimestamps]; ok {
		jsonEntries[jsonMetaSentinel] = map[string]interface{}{
			`updated`: tree.TimestampToDecimal(updated).Decimal.String(),
		}
	}
	for i, col := range columns {
		datum := row[i]
		if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[col.Name], err = tree.AsJSON(datum.Datum)
		if err != nil {
			return nil, err
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
func (e *jsonEncoder) EncodeResolvedTimestamp(_ string, resolved hlc.Timestamp) ([]byte, error) {
	resolvedMetaRaw := map[string]interface{}{
		jsonMetaSentinel: map[string]interface{}{
			`resolved`: tree.TimestampToDecimal(resolved).Decimal.String(),
		},
	}
	return gojson.Marshal(resolvedMetaRaw)
}

// confluentAvroEncoder encodes changefeed entries as Avro's binary or textual
// JSON format. Keys are the primary key columns in a record. Values are all
// columns in a record.
type confluentAvroEncoder struct {
	registryURL string
	opts        map[string]string

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
	registryURL := opts[optConfluentSchemaRegistry]
	if len(registryURL) == 0 {
		return nil, errors.Errorf(`WITH option %s is required for %s=%s`,
			optConfluentSchemaRegistry, optFormat, optFormatAvro)
	}
	e := &confluentAvroEncoder{
		registryURL:   registryURL,
		opts:          opts,
		keyCache:      make(map[tableIDAndVersion]confluentRegisteredKeySchema),
		valueCache:    make(map[tableIDAndVersion]confluentRegisteredEnvelopeSchema),
		resolvedCache: make(map[string]confluentRegisteredEnvelopeSchema),
	}

	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeKey(
	tableDesc *sqlbase.TableDescriptor, row sqlbase.EncDatumRow,
) ([]byte, error) {
	cacheKey := makeTableIDAndVersion(tableDesc.ID, tableDesc.Version)
	registered, ok := e.keyCache[cacheKey]
	if !ok {
		var err error
		registered.schema, err = indexToAvroSchema(tableDesc, &tableDesc.PrimaryIndex)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(tableDesc.Name) + confluentSubjectSuffixKey
		registered.registryID, err = e.register(&registered.schema.avroRecord, subject)
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
	return registered.schema.BinaryFromRow(header, row)
}

// EncodeValue implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeValue(
	tableDesc *sqlbase.TableDescriptor, row sqlbase.EncDatumRow, updated hlc.Timestamp,
) ([]byte, error) {
	cacheKey := makeTableIDAndVersion(tableDesc.ID, tableDesc.Version)
	registered, ok := e.valueCache[cacheKey]
	if !ok {
		afterDataSchema, err := tableToAvroSchema(tableDesc)
		if err != nil {
			return nil, err
		}

		opts := avroEnvelopeOpts{afterField: true}
		_, opts.updatedField = e.opts[optUpdatedTimestamps]
		registered.schema, err = envelopeToAvroSchema(tableDesc.Name, opts, afterDataSchema)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(tableDesc.Name) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(&registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.valueCache[cacheKey] = registered
	}
	var meta avroMetadata
	if registered.schema.opts.updatedField {
		meta = map[string]interface{}{
			`updated`: updated,
		}
	}
	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		confluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, row)
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeResolvedTimestamp(
	topic string, resolved hlc.Timestamp,
) ([]byte, error) {
	registered, ok := e.resolvedCache[topic]
	if !ok {
		var opts avroEnvelopeOpts
		_, opts.resolvedField = e.opts[optResolvedTimestamps]
		var err error
		registered.schema, err = envelopeToAvroSchema(topic, opts, nil /* after */)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(topic) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(&registered.schema.avroRecord, subject)
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

func (e *confluentAvroEncoder) register(schema *avroRecord, subject string) (int32, error) {
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

	resp, err := http.Post(url.String(), confluentSchemaContentType, &buf)
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

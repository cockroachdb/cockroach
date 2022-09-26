// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

const (
	confluentSubjectSuffixKey   = `-key`
	confluentSubjectSuffixValue = `-value`
)

// confluentAvroEncoder encodes changefeed entries as Avro's binary or textual
// JSON format. Keys are the primary key columns in a record. Values are all
// columns in a record.
type confluentAvroEncoder struct {
	schemaRegistry            schemaRegistry
	schemaPrefix              string
	updatedField, beforeField bool
	virtualColumnVisibility   changefeedbase.VirtualColumnVisibility
	targets                   changefeedbase.Targets
	envelopeType              changefeedbase.EnvelopeType

	keyCache   *cache.UnorderedCache // [tableIDAndVersion]confluentRegisteredKeySchema
	valueCache *cache.UnorderedCache // [tableIDAndVersionPair]confluentRegisteredEnvelopeSchema

	// resolvedCache doesn't need to be bounded like the other caches because the number of topics
	// is fixed per changefeed.
	resolvedCache map[string]confluentRegisteredEnvelopeSchema
}

type tableIDAndVersion struct {
	tableID  descpb.ID
	version  descpb.DescriptorVersion
	familyID descpb.FamilyID
}
type tableIDAndVersionPair [2]tableIDAndVersion // [before, after]

type confluentRegisteredKeySchema struct {
	schema     *avroDataRecord
	registryID int32
}

type confluentRegisteredEnvelopeSchema struct {
	schema     *avroEnvelopeRecord
	registryID int32
}

var _ Encoder = &confluentAvroEncoder{}

var encoderCacheConfig = cache.Config{
	Policy: cache.CacheFIFO,
	// TODO: If we find ourselves thrashing here in changefeeds on many tables,
	// we can improve performance by eagerly evicting versions using Resolved notifications.
	// An old version with a timestamp entirely before a notification can be safely evicted.
	ShouldEvict: func(size int, _ interface{}, _ interface{}) bool { return size > 1024 },
}

func newConfluentAvroEncoder(
	opts changefeedbase.EncodingOptions, targets changefeedbase.Targets,
) (*confluentAvroEncoder, error) {
	e := &confluentAvroEncoder{
		schemaPrefix:            opts.AvroSchemaPrefix,
		targets:                 targets,
		virtualColumnVisibility: opts.VirtualColumns,
		envelopeType:            opts.Envelope,
	}

	e.updatedField = opts.UpdatedTimestamps
	e.beforeField = opts.Diff

	// TODO: Implement this.
	if opts.KeyInValue {
		return nil, errors.Errorf(`%s is not supported with %s=%s`,
			changefeedbase.OptKeyInValue, changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}

	// TODO: Implement this.
	if opts.TopicInValue {
		return nil, errors.Errorf(`%s is not supported with %s=%s`,
			changefeedbase.OptTopicInValue, changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}
	if len(opts.SchemaRegistryURI) == 0 {
		return nil, errors.Errorf(`WITH option %s is required for %s=%s`,
			changefeedbase.OptConfluentSchemaRegistry, changefeedbase.OptFormat, changefeedbase.OptFormatAvro)
	}

	reg, err := newConfluentSchemaRegistry(opts.SchemaRegistryURI)
	if err != nil {
		return nil, err
	}

	e.schemaRegistry = reg
	e.keyCache = cache.NewUnorderedCache(encoderCacheConfig)
	e.valueCache = cache.NewUnorderedCache(encoderCacheConfig)
	e.resolvedCache = make(map[string]confluentRegisteredEnvelopeSchema)
	return e, nil
}

// Get the raw SQL-formatted string for a table name
// and apply full_table_name and avro_schema_prefix options
func (e *confluentAvroEncoder) rawTableName(eventMeta cdcevent.Metadata) (string, error) {
	target, found := e.targets.FindByTableIDAndFamilyName(eventMeta.TableID, eventMeta.FamilyName)
	if !found {
		return eventMeta.TableName, errors.Newf("Could not find Target for %s", eventMeta)
	}
	switch target.Type {
	case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
		return e.schemaPrefix + string(target.StatementTimeName), nil
	case jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
		return fmt.Sprintf("%s%s.%s", e.schemaPrefix, target.StatementTimeName, eventMeta.FamilyName), nil
	case jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY:
		return fmt.Sprintf("%s%s.%s", e.schemaPrefix, target.StatementTimeName, target.FamilyName), nil
	default:
		return "", errors.AssertionFailedf("Found a matching target with unimplemented type %s", target.Type)
	}
}

// EncodeKey implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeKey(ctx context.Context, row cdcevent.Row) ([]byte, error) {
	// No familyID in the cache key for keys because it's the same schema for all families
	cacheKey := tableIDAndVersion{tableID: row.TableID, version: row.Version}

	var registered confluentRegisteredKeySchema
	v, ok := e.keyCache.Get(cacheKey)
	if ok {
		registered = v.(confluentRegisteredKeySchema)
		if err := registered.schema.refreshTypeMetadata(row); err != nil {
			return nil, err
		}
	} else {
		var err error
		tableName, err := e.rawTableName(row.Metadata)
		if err != nil {
			return nil, err
		}
		registered.schema, err = primaryIndexToAvroSchema(row, tableName, e.schemaPrefix)
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
		e.keyCache.Add(cacheKey, registered)
	}

	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		changefeedbase.ConfluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, row.ForEachKeyColumn())
}

// EncodeValue implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeValue(
	ctx context.Context, evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	if e.envelopeType == changefeedbase.OptEnvelopeKeyOnly {
		return nil, nil
	}

	var cacheKey tableIDAndVersionPair
	if e.beforeField && prevRow.IsInitialized() {
		cacheKey[0] = tableIDAndVersion{
			tableID: prevRow.TableID, version: prevRow.Version, familyID: prevRow.FamilyID,
		}
	}
	cacheKey[1] = tableIDAndVersion{
		tableID: updatedRow.TableID, version: updatedRow.Version, familyID: updatedRow.FamilyID,
	}

	var registered confluentRegisteredEnvelopeSchema
	v, ok := e.valueCache.Get(cacheKey)
	if ok {
		registered = v.(confluentRegisteredEnvelopeSchema)
		if err := registered.schema.after.refreshTypeMetadata(updatedRow); err != nil {
			return nil, err
		}
		if prevRow.IsInitialized() && registered.schema.before != nil {
			if err := registered.schema.before.refreshTypeMetadata(prevRow); err != nil {
				return nil, err
			}
		}
	} else {
		var beforeDataSchema, afterDataSchema, recordDataSchema *avroDataRecord
		if e.beforeField && prevRow.IsInitialized() {
			var err error
			beforeDataSchema, err = tableToAvroSchema(prevRow, `before`, e.schemaPrefix)
			if err != nil {
				return nil, err
			}
		}

		currentSchema, err := tableToAvroSchema(updatedRow, avroSchemaNoSuffix, e.schemaPrefix)
		if err != nil {
			return nil, err
		}

		var opts avroEnvelopeOpts

		// In the wrapped envelope, row data goes in the "after" field. In the raw envelope,
		// it goes in the "record" field. In the "key_only" envelope it's omitted.
		// This means metadata can safely go at the top level as there are never arbitrary column names
		// for it to conflict with.
		if e.envelopeType == changefeedbase.OptEnvelopeWrapped {
			opts = avroEnvelopeOpts{afterField: true, beforeField: e.beforeField, updatedField: e.updatedField}
			afterDataSchema = currentSchema
		} else {
			opts = avroEnvelopeOpts{recordField: true, updatedField: e.updatedField}
			recordDataSchema = currentSchema
		}

		name, err := e.rawTableName(updatedRow.Metadata)
		if err != nil {
			return nil, err
		}
		registered.schema, err = envelopeToAvroSchema(name, opts, beforeDataSchema, afterDataSchema, recordDataSchema, e.schemaPrefix)

		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(name) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(ctx, &registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		e.valueCache.Add(cacheKey, registered)
	}

	var meta avroMetadata
	if registered.schema.opts.updatedField {
		meta = map[string]interface{}{
			`updated`: evCtx.updated,
		}
	}

	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		changefeedbase.ConfluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, prevRow, updatedRow, updatedRow)
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeResolvedTimestamp(
	ctx context.Context, topic string, resolved hlc.Timestamp,
) ([]byte, error) {
	registered, ok := e.resolvedCache[topic]
	if !ok {
		opts := avroEnvelopeOpts{resolvedField: true}
		var err error
		registered.schema, err = envelopeToAvroSchema(topic, opts, nil /* before */, nil /* after */, nil /* record */, e.schemaPrefix /* namespace */)
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
	var nilRow cdcevent.Row
	return registered.schema.BinaryFromRow(header, meta, nilRow, nilRow, nilRow)
}

func (e *confluentAvroEncoder) register(
	ctx context.Context, schema *avroRecord, subject string,
) (int32, error) {
	return e.schemaRegistry.RegisterSchemaForSubject(ctx, subject, schema.codec.Schema())
}

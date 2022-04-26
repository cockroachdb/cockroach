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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
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
	schemaRegistry                     schemaRegistry
	schemaPrefix                       string
	updatedField, beforeField, keyOnly bool
	virtualColumnVisibility            string
	targets                            []jobspb.ChangefeedTargetSpecification

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
	opts map[string]string, targets []jobspb.ChangefeedTargetSpecification,
) (*confluentAvroEncoder, error) {
	e := &confluentAvroEncoder{
		schemaPrefix:            opts[changefeedbase.OptAvroSchemaPrefix],
		targets:                 targets,
		virtualColumnVisibility: opts[changefeedbase.OptVirtualColumns],
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
	e.keyCache = cache.NewUnorderedCache(encoderCacheConfig)
	e.valueCache = cache.NewUnorderedCache(encoderCacheConfig)
	e.resolvedCache = make(map[string]confluentRegisteredEnvelopeSchema)
	return e, nil
}

// Get the raw SQL-formatted string for a table name
// and apply full_table_name and avro_schema_prefix options
func (e *confluentAvroEncoder) rawTableName(
	desc catalog.TableDescriptor, familyID descpb.FamilyID,
) (string, error) {
	for _, target := range e.targets {
		if target.TableID == desc.GetID() {
			switch target.Type {
			case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
				return e.schemaPrefix + target.StatementTimeName, nil
			case jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
				family, err := desc.FindFamilyByID(familyID)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s%s.%s", e.schemaPrefix, target.StatementTimeName, family.Name), nil
			case jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY:
				family, err := desc.FindFamilyByID(familyID)
				if err != nil {
					return "", err
				}
				if family.Name != target.FamilyName {
					// Not the right target specification for this family
					continue
				}
				return fmt.Sprintf("%s%s.%s", e.schemaPrefix, target.StatementTimeName, target.FamilyName), nil
			default:
				// fall through to error
			}
			return "", errors.AssertionFailedf("Found a matching target with unimplemented type %s", target.Type)
		}
	}
	return desc.GetName(), errors.Newf("Could not find TargetSpecification for descriptor %v", desc)
}

// EncodeKey implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeKey(ctx context.Context, row encodeRow) ([]byte, error) {
	// No familyID in the cache key for keys because it's the same schema for all families
	cacheKey := tableIDAndVersion{tableID: row.tableDesc.GetID(), version: row.tableDesc.GetVersion()}

	var registered confluentRegisteredKeySchema
	v, ok := e.keyCache.Get(cacheKey)
	if ok {
		registered = v.(confluentRegisteredKeySchema)
		registered.schema.refreshTypeMetadata(row.tableDesc)
	} else {
		var err error
		tableName, err := e.rawTableName(row.tableDesc, row.familyID)
		if err != nil {
			return nil, err
		}
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
		e.keyCache.Add(cacheKey, registered)
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
		cacheKey[0] = tableIDAndVersion{
			tableID: row.prevTableDesc.GetID(), version: row.prevTableDesc.GetVersion(), familyID: row.prevFamilyID,
		}
	}
	cacheKey[1] = tableIDAndVersion{
		tableID: row.tableDesc.GetID(), version: row.tableDesc.GetVersion(), familyID: row.familyID,
	}

	var registered confluentRegisteredEnvelopeSchema
	v, ok := e.valueCache.Get(cacheKey)
	if ok {
		registered = v.(confluentRegisteredEnvelopeSchema)
		registered.schema.after.refreshTypeMetadata(row.tableDesc)
		if row.prevTableDesc != nil && registered.schema.before != nil {
			registered.schema.before.refreshTypeMetadata(row.prevTableDesc)
		}
	} else {
		var beforeDataSchema *avroDataRecord
		if e.beforeField && row.prevTableDesc != nil {
			var err error
			beforeDataSchema, err = tableToAvroSchema(row.prevTableDesc, row.prevFamilyID, `before`, e.schemaPrefix, e.virtualColumnVisibility)
			if err != nil {
				return nil, err
			}
		}

		afterDataSchema, err := tableToAvroSchema(row.tableDesc, row.familyID, avroSchemaNoSuffix, e.schemaPrefix, e.virtualColumnVisibility)
		if err != nil {
			return nil, err
		}

		opts := avroEnvelopeOpts{afterField: true, beforeField: e.beforeField, updatedField: e.updatedField}
		name, err := e.rawTableName(row.tableDesc, row.familyID)
		if err != nil {
			return nil, err
		}
		registered.schema, err = envelopeToAvroSchema(name, opts, beforeDataSchema, afterDataSchema, e.schemaPrefix)

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

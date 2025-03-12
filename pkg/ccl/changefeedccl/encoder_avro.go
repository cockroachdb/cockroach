// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/avro"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	schemaRegistry                         schemaRegistry
	schemaPrefix                           string
	updatedField, beforeField, sourceField bool
	mvccTimestampField                     bool
	virtualColumnVisibility                changefeedbase.VirtualColumnVisibility
	targets                                changefeedbase.Targets
	envelopeType                           changefeedbase.EnvelopeType
	customKeyColumn                        string
	headersJSONColumnName                  string

	keyCache   *cache.UnorderedCache // [tableIDAndVersion]confluentRegisteredKeySchema
	valueCache *cache.UnorderedCache // [tableIDAndVersionPair]confluentRegisteredEnvelopeSchema

	enrichedSourceProvider *enrichedSourceProvider

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
	schema     *avro.DataRecord
	registryID int32
}

type confluentRegisteredEnvelopeSchema struct {
	schema     *avro.EnvelopeRecord
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
	opts changefeedbase.EncodingOptions,
	targets changefeedbase.Targets,
	p externalConnectionProvider,
	sliMetrics *sliMetrics,
	enrichedSourceProvider *enrichedSourceProvider,
) (*confluentAvroEncoder, error) {
	e := &confluentAvroEncoder{
		schemaPrefix:            opts.AvroSchemaPrefix,
		targets:                 targets,
		virtualColumnVisibility: opts.VirtualColumns,
		envelopeType:            opts.Envelope,
	}

	e.updatedField = opts.UpdatedTimestamps
	e.beforeField = opts.Diff
	e.sourceField = inSet(changefeedbase.EnrichedPropertySource, opts.EnrichedProperties)
	e.customKeyColumn = opts.CustomKeyColumn
	e.headersJSONColumnName = opts.HeadersJSONColName
	e.mvccTimestampField = opts.MVCCTimestamps
	e.enrichedSourceProvider = enrichedSourceProvider

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

	reg, err := newConfluentSchemaRegistry(opts.SchemaRegistryURI, p, sliMetrics)
	if err != nil {
		return nil, err
	}

	e.schemaRegistry = reg
	e.keyCache = cache.NewUnorderedCache(encoderCacheConfig)
	e.valueCache = cache.NewUnorderedCache(encoderCacheConfig)
	e.resolvedCache = make(map[string]confluentRegisteredEnvelopeSchema)
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeKey(ctx context.Context, row cdcevent.Row) ([]byte, error) {
	// No familyID in the cache key for keys because it's the same schema for all families
	cacheKey := tableIDAndVersion{tableID: row.TableID, version: row.Version}

	var registered confluentRegisteredKeySchema
	v, ok := e.keyCache.Get(cacheKey)
	if ok {
		registered = v.(confluentRegisteredKeySchema)
		if err := registered.schema.RefreshTypeMetadata(row); err != nil {
			return nil, err
		}
	} else {
		var err error
		tableName, err := getTableName(e.targets, e.schemaPrefix, row.Metadata)
		if err != nil {
			return nil, err
		}
		if e.customKeyColumn == "" {
			registered.schema, err = avro.PrimaryIndexToAvroSchema(row, tableName, e.schemaPrefix)
			if err != nil {
				return nil, err
			}
		} else {
			it, err := row.DatumNamed(e.customKeyColumn)
			if err != nil {
				return nil, err
			}
			registered.schema, err = avro.NewSchemaForRow(it, changefeedbase.SQLNameToAvroName(tableName), e.schemaPrefix)
			if err != nil {
				return nil, err
			}
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := changefeedbase.SQLNameToKafkaName(tableName) + confluentSubjectSuffixKey
		registered.registryID, err = e.register(ctx, &registered.schema.Record, subject)
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
	if e.customKeyColumn != "" {
		it, err := row.DatumNamed(e.customKeyColumn)
		if err != nil {
			return nil, err
		}
		return registered.schema.BinaryFromRow(header, it)
	}
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
		if prevRow.IsInitialized() && registered.schema.Before != nil {
			if err := registered.schema.Before.RefreshTypeMetadata(prevRow); err != nil {
				return nil, err
			}
		}
		if registered.schema.After != nil {
			if err := registered.schema.After.RefreshTypeMetadata(updatedRow); err != nil {
				return nil, err
			}
		}
		if registered.schema.Rec != nil {
			if err := registered.schema.Rec.RefreshTypeMetadata(updatedRow); err != nil {
				return nil, err
			}
		}
	} else {
		var beforeDataSchema, afterDataSchema, recordDataSchema *avro.DataRecord
		var sourceDataSchema *avro.FunctionalRecord
		if e.beforeField && prevRow.IsInitialized() {
			var err error
			beforeDataSchema, err = avro.TableToAvroSchema(prevRow, `before`, e.schemaPrefix, e.headersJSONColumnName)
			if err != nil {
				return nil, err
			}
		}

		currentSchema, err := avro.TableToAvroSchema(updatedRow, avro.SchemaNoSuffix, e.schemaPrefix, e.headersJSONColumnName)
		if err != nil {
			return nil, err
		}

		var opts avro.EnvelopeOpts

		// In the wrapped envelope, row data goes in the "after" field. In the raw envelope,
		// it goes in the "record" field. In the "key_only" envelope it's omitted.
		// This means metadata can safely go at the top level as there are never arbitrary column names
		// for it to conflict with.
		switch e.envelopeType {

		case changefeedbase.OptEnvelopeWrapped:
			opts = avro.EnvelopeOpts{AfterField: true, BeforeField: e.beforeField, UpdatedField: e.updatedField, MVCCTimestampField: e.mvccTimestampField}
			afterDataSchema = currentSchema
		case changefeedbase.OptEnvelopeBare:
			opts = avro.EnvelopeOpts{RecordField: true, UpdatedField: e.updatedField, MVCCTimestampField: e.mvccTimestampField}
			recordDataSchema = currentSchema
		case changefeedbase.OptEnvelopeEnriched:
			afterDataSchema = currentSchema
			opts = avro.EnvelopeOpts{AfterField: true, BeforeField: e.beforeField, OpField: true, TsField: true, SourceField: e.sourceField}
			if e.sourceField {
				if sourceDataSchema, err = e.enrichedSourceProvider.GetAvro(updatedRow, e.schemaPrefix); err != nil {
					return nil, err
				}
			}
		// key_only handled above, and row is not supported in avro
		default:
			return nil, errors.AssertionFailedf(`unknown envelope type: %s`, e.envelopeType)
		}

		name, err := getTableName(e.targets, e.schemaPrefix, updatedRow.Metadata)
		if err != nil {
			return nil, err
		}
		registered.schema, err = avro.NewEnvelopeRecord(name, opts, beforeDataSchema, afterDataSchema, recordDataSchema, sourceDataSchema, e.schemaPrefix)

		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := changefeedbase.SQLNameToKafkaName(name) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(ctx, &registered.schema.Record, subject)
		if err != nil {
			return nil, err
		}
		e.valueCache.Add(cacheKey, registered)
	}

	meta := avro.Metadata{}
	if registered.schema.Opts.UpdatedField {
		meta[`updated`] = evCtx.updated
	}
	if registered.schema.Opts.MVCCTimestampField {
		meta[`mvcc_timestamp`] = evCtx.mvcc
	}
	if registered.schema.Opts.OpField {
		meta[`op`] = string(deduceOp(updatedRow, prevRow))
	}
	if registered.schema.Opts.TsField {
		meta[`ts_ns`] = timeutil.Now().UnixNano()
	}

	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		changefeedbase.ConfluentAvroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, prevRow, updatedRow, updatedRow, "" /* omitColumn */)
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *confluentAvroEncoder) EncodeResolvedTimestamp(
	ctx context.Context, topic string, resolved hlc.Timestamp,
) ([]byte, error) {
	registered, ok := e.resolvedCache[topic]
	if !ok {
		opts := avro.EnvelopeOpts{ResolvedField: true}
		var err error
		registered.schema, err = avro.NewEnvelopeRecord(topic, opts, nil /* before */, nil /* after */, nil /* record */, nil /* source */, e.schemaPrefix /* namespace */)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := changefeedbase.SQLNameToKafkaName(topic) + confluentSubjectSuffixValue
		registered.registryID, err = e.register(ctx, &registered.schema.Record, subject)
		if err != nil {
			return nil, err
		}

		e.resolvedCache[topic] = registered
	}
	var meta avro.Metadata
	if registered.schema.Opts.ResolvedField {
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
	return registered.schema.BinaryFromRow(header, meta, nilRow, nilRow, nilRow, "")
}

func (e *confluentAvroEncoder) register(
	ctx context.Context, schema *avro.Record, subject string,
) (int32, error) {
	return e.schemaRegistry.RegisterSchemaForSubject(ctx, subject, schema.Schema())
}

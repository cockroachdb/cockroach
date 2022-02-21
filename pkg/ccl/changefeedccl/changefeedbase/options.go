// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import "github.com/cockroachdb/cockroach/pkg/sql"

// EnvelopeType configures the information in the changefeed events for a row.
type EnvelopeType string

// FormatType configures the encoding format.
type FormatType string

// OnErrorType configures the job behavior when an error occurs.
type OnErrorType string

// SchemaChangeEventClass defines a set of schema change event types which
// trigger the action defined by the SchemaChangeEventPolicy.
type SchemaChangeEventClass string

// SchemaChangePolicy defines the behavior of a changefeed when a schema
// change event which is a member of the changefeed's schema change events.
type SchemaChangePolicy string

// VirtualColumnVisibility defines the behaviour of how the changefeed will
// include virtual columns in an event
type VirtualColumnVisibility string

// Constants for the options.
const (
	OptAvroSchemaPrefix         = `avro_schema_prefix`
	OptConfluentSchemaRegistry  = `confluent_schema_registry`
	OptCursor                   = `cursor`
	OptEnvelope                 = `envelope`
	OptFormat                   = `format`
	OptFullTableName            = `full_table_name`
	OptKeyInValue               = `key_in_value`
	OptTopicInValue             = `topic_in_value`
	OptResolvedTimestamps       = `resolved`
	OptMinCheckpointFrequency   = `min_checkpoint_frequency`
	OptUpdatedTimestamps        = `updated`
	OptMVCCTimestamps           = `mvcc_timestamp`
	OptDiff                     = `diff`
	OptCompression              = `compression`
	OptSchemaChangeEvents       = `schema_change_events`
	OptSchemaChangePolicy       = `schema_change_policy`
	OptSplitColumnFamilies      = `split_column_families`
	OptProtectDataFromGCOnPause = `protect_data_from_gc_on_pause`
	OptWebhookAuthHeader        = `webhook_auth_header`
	OptWebhookClientTimeout     = `webhook_client_timeout`
	OptOnError                  = `on_error`
	OptMetricsScope             = `metrics_label`
	OptVirtualColumns           = `virtual_columns`

	OptVirtualColumnsOmitted VirtualColumnVisibility = `omitted`
	OptVirtualColumnsNull    VirtualColumnVisibility = `null`

	// OptSchemaChangeEventClassColumnChange corresponds to all schema change
	// events which add or remove any column.
	OptSchemaChangeEventClassColumnChange SchemaChangeEventClass = `column_changes`
	// OptSchemaChangeEventClassDefault corresponds to all schema change
	// events which add a column with a default value or remove any column.
	OptSchemaChangeEventClassDefault SchemaChangeEventClass = `default`

	// OptSchemaChangePolicyBackfill indicates that when a schema change event
	// occurs, a full table backfill should occur.
	OptSchemaChangePolicyBackfill SchemaChangePolicy = `backfill`
	// OptSchemaChangePolicyNoBackfill indicates that when a schema change event occurs
	// no backfill should occur and the changefeed should continue.
	OptSchemaChangePolicyNoBackfill SchemaChangePolicy = `nobackfill`
	// OptSchemaChangePolicyStop indicates that when a schema change event occurs
	// the changefeed should resolve all data up to when it occurred and then
	// exit with an error indicating the HLC timestamp of the change from which
	// the user could continue.
	OptSchemaChangePolicyStop SchemaChangePolicy = `stop`
	// OptSchemaChangePolicyIgnore indicates that all schema change events should
	// be ignored.
	OptSchemaChangePolicyIgnore SchemaChangePolicy = `ignore`

	// OptInitialScan enables an initial scan. This is the default when no
	// cursor is specified, leading to an initial scan at the statement time of
	// the creation of the changeffed. If used in conjunction with a cursor,
	// an initial scan will be performed at the cursor timestamp.
	OptInitialScan = `initial_scan`
	// OptInitialScan enables an initial scan. This is the default when a
	// cursor is specified. This option is useful to create a changefeed which
	// subscribes only to new messages.
	OptNoInitialScan = `no_initial_scan`
	// Sentinel value to indicate that all resolved timestamp events should be emitted.
	OptEmitAllResolvedTimestamps = ``

	OptEnvelopeKeyOnly       EnvelopeType = `key_only`
	OptEnvelopeRow           EnvelopeType = `row`
	OptEnvelopeDeprecatedRow EnvelopeType = `deprecated_row`
	OptEnvelopeWrapped       EnvelopeType = `wrapped`

	OptFormatJSON FormatType = `json`
	OptFormatAvro FormatType = `avro`

	OptFormatNative FormatType = `native`

	OptOnErrorFail  OnErrorType = `fail`
	OptOnErrorPause OnErrorType = `pause`

	DeprecatedOptFormatAvro                   = `experimental_avro`
	DeprecatedSinkSchemeCloudStorageAzure     = `experimental-azure`
	DeprecatedSinkSchemeCloudStorageGCS       = `experimental-gs`
	DeprecatedSinkSchemeCloudStorageHTTP      = `experimental-http`
	DeprecatedSinkSchemeCloudStorageHTTPS     = `experimental-https`
	DeprecatedSinkSchemeCloudStorageNodelocal = `experimental-nodelocal`
	DeprecatedSinkSchemeCloudStorageS3        = `experimental-s3`

	// OptKafkaSinkConfig is a JSON configuration for kafka sink (kafkaSinkConfig).
	OptKafkaSinkConfig   = `kafka_sink_config`
	OptWebhookSinkConfig = `webhook_sink_config`

	// OptSink allows users to alter the Sink URI of an existing changefeed.
	// Note that this option is only allowed for alter changefeed statements.
	OptSink = `sink`

	SinkParamCACert                 = `ca_cert`
	SinkParamClientCert             = `client_cert`
	SinkParamClientKey              = `client_key`
	SinkParamFileSize               = `file_size`
	SinkParamPartitionFormat        = `partition_format`
	SinkParamSchemaTopic            = `schema_topic`
	SinkParamTLSEnabled             = `tls_enabled`
	SinkParamSkipTLSVerify          = `insecure_tls_skip_verify`
	SinkParamTopicPrefix            = `topic_prefix`
	SinkParamTopicName              = `topic_name`
	SinkSchemeCloudStorageAzure     = `azure`
	SinkSchemeCloudStorageGCS       = `gs`
	SinkSchemeCloudStorageHTTP      = `http`
	SinkSchemeCloudStorageHTTPS     = `https`
	SinkSchemeCloudStorageNodelocal = `nodelocal`
	SinkSchemeCloudStorageS3        = `s3`
	SinkSchemeExperimentalSQL       = `experimental-sql`
	SinkSchemeHTTP                  = `http`
	SinkSchemeHTTPS                 = `https`
	SinkSchemeKafka                 = `kafka`
	SinkSchemeNull                  = `null`
	SinkSchemeWebhookHTTP           = `webhook-http`
	SinkSchemeWebhookHTTPS          = `webhook-https`
	SinkParamSASLEnabled            = `sasl_enabled`
	SinkParamSASLHandshake          = `sasl_handshake`
	SinkParamSASLUser               = `sasl_user`
	SinkParamSASLPassword           = `sasl_password`
	SinkParamSASLMechanism          = `sasl_mechanism`

	RegistryParamCACert = `ca_cert`

	// Topics is used to store the topics generated by the sink in the options
	// struct so that they can be displayed in the show changefeed jobs query.
	// Hence, this option is not available to users
	Topics = `topics`
)

// ChangefeedOptionExpectValues is used to parse changefeed options using
// PlanHookState.TypeAsStringOpts().
var ChangefeedOptionExpectValues = map[string]sql.KVStringOptValidate{
	OptAvroSchemaPrefix:         sql.KVStringOptRequireValue,
	OptConfluentSchemaRegistry:  sql.KVStringOptRequireValue,
	OptCursor:                   sql.KVStringOptRequireValue,
	OptEnvelope:                 sql.KVStringOptRequireValue,
	OptFormat:                   sql.KVStringOptRequireValue,
	OptFullTableName:            sql.KVStringOptRequireNoValue,
	OptKeyInValue:               sql.KVStringOptRequireNoValue,
	OptTopicInValue:             sql.KVStringOptRequireNoValue,
	OptResolvedTimestamps:       sql.KVStringOptAny,
	OptMinCheckpointFrequency:   sql.KVStringOptRequireValue,
	OptUpdatedTimestamps:        sql.KVStringOptRequireNoValue,
	OptMVCCTimestamps:           sql.KVStringOptRequireNoValue,
	OptDiff:                     sql.KVStringOptRequireNoValue,
	OptCompression:              sql.KVStringOptRequireValue,
	OptSchemaChangeEvents:       sql.KVStringOptRequireValue,
	OptSchemaChangePolicy:       sql.KVStringOptRequireValue,
	OptSplitColumnFamilies:      sql.KVStringOptRequireNoValue,
	OptInitialScan:              sql.KVStringOptRequireNoValue,
	OptNoInitialScan:            sql.KVStringOptRequireNoValue,
	OptProtectDataFromGCOnPause: sql.KVStringOptRequireNoValue,
	OptKafkaSinkConfig:          sql.KVStringOptRequireValue,
	OptWebhookSinkConfig:        sql.KVStringOptRequireValue,
	OptWebhookAuthHeader:        sql.KVStringOptRequireValue,
	OptWebhookClientTimeout:     sql.KVStringOptRequireValue,
	OptOnError:                  sql.KVStringOptRequireValue,
	OptMetricsScope:             sql.KVStringOptRequireValue,
	OptVirtualColumns:           sql.KVStringOptRequireValue,
}

func makeStringSet(opts ...string) map[string]struct{} {
	res := make(map[string]struct{}, len(opts))
	for _, opt := range opts {
		res[opt] = struct{}{}
	}
	return res
}

// CommonOptions is options common to all sinks
var CommonOptions = makeStringSet(OptCursor, OptEnvelope,
	OptFormat, OptFullTableName,
	OptKeyInValue, OptTopicInValue,
	OptResolvedTimestamps, OptUpdatedTimestamps,
	OptMVCCTimestamps, OptDiff, OptSplitColumnFamilies,
	OptSchemaChangeEvents, OptSchemaChangePolicy,
	OptProtectDataFromGCOnPause, OptOnError,
	OptInitialScan, OptNoInitialScan,
	OptMinCheckpointFrequency, OptMetricsScope, OptVirtualColumns, Topics)

// SQLValidOptions is options exclusive to SQL sink
var SQLValidOptions map[string]struct{} = nil

// KafkaValidOptions is options exclusive to Kafka sink
var KafkaValidOptions = makeStringSet(OptAvroSchemaPrefix, OptConfluentSchemaRegistry, OptKafkaSinkConfig)

// CloudStorageValidOptions is options exclusive to cloud storage sink
var CloudStorageValidOptions = makeStringSet(OptCompression)

// WebhookValidOptions is options exclusive to webhook sink
var WebhookValidOptions = makeStringSet(OptWebhookAuthHeader, OptWebhookClientTimeout, OptWebhookSinkConfig)

// PubsubValidOptions is options exclusice to pubsub sink
var PubsubValidOptions = makeStringSet()

// CaseInsensitiveOpts options which supports case Insensitive value
var CaseInsensitiveOpts = makeStringSet(OptFormat, OptEnvelope, OptCompression, OptSchemaChangeEvents, OptSchemaChangePolicy, OptOnError)

// NoLongerExperimental aliases options prefixed with experimental that no longer need to be
var NoLongerExperimental = map[string]string{
	DeprecatedOptFormatAvro:                   string(OptFormatAvro),
	DeprecatedSinkSchemeCloudStorageAzure:     SinkSchemeCloudStorageAzure,
	DeprecatedSinkSchemeCloudStorageGCS:       SinkSchemeCloudStorageGCS,
	DeprecatedSinkSchemeCloudStorageHTTP:      SinkSchemeCloudStorageHTTP,
	DeprecatedSinkSchemeCloudStorageHTTPS:     SinkSchemeCloudStorageHTTPS,
	DeprecatedSinkSchemeCloudStorageNodelocal: SinkSchemeCloudStorageNodelocal,
	DeprecatedSinkSchemeCloudStorageS3:        SinkSchemeCloudStorageS3,
}

// AlterChangefeedUnsupportedOptions are changefeed options that we do not allow
// users to alter
var AlterChangefeedUnsupportedOptions = makeStringSet(OptCursor, OptInitialScan, OptNoInitialScan)

// AlterChangefeedOptionExpectValues is used to parse alter changefeed options
// using PlanHookState.TypeAsStringOpts().
var AlterChangefeedOptionExpectValues = func() map[string]sql.KVStringOptValidate {
	alterChangefeedOptions := make(map[string]sql.KVStringOptValidate, len(ChangefeedOptionExpectValues)+1)
	for key, value := range ChangefeedOptionExpectValues {
		alterChangefeedOptions[key] = value
	}
	alterChangefeedOptions[OptSink] = sql.KVStringOptRequireValue
	return alterChangefeedOptions
}()

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

// SchemaChangeEventClass defines a set of schema change event types which
// trigger the action defined by the SchemaChangeEventPolicy.
type SchemaChangeEventClass string

// SchemaChangePolicy defines the behavior of a changefeed when a schema
// change event which is a member of the changefeed's schema change events.
type SchemaChangePolicy string

// Constants for the options.
const (
	OptConfluentSchemaRegistry  = `confluent_schema_registry`
	OptCursor                   = `cursor`
	OptEnvelope                 = `envelope`
	OptFormat                   = `format`
	OptKeyInValue               = `key_in_value`
	OptResolvedTimestamps       = `resolved`
	OptUpdatedTimestamps        = `updated`
	OptDiff                     = `diff`
	OptCompression              = `compression`
	OptSchemaChangeEvents       = `schema_change_events`
	OptSchemaChangePolicy       = `schema_change_policy`
	OptProtectDataFromGCOnPause = `protect_data_from_gc_on_pause`

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

	// OptInitialScan enables an initial scan. This is the default when no
	// cursor is specified, leading to an initial scan at the statement time of
	// the creation of the changeffed. If used in conjunction with a cursor,
	// an initial scan will be performed at the cursor timestamp.
	OptInitialScan = `initial_scan`
	// OptInitialScan enables an initial scan. This is the default when a
	// cursor is specified. This option is useful to create a changefeed which
	// subscribes only to new messages.
	OptNoInitialScan = `no_initial_scan`

	OptEnvelopeKeyOnly       EnvelopeType = `key_only`
	OptEnvelopeRow           EnvelopeType = `row`
	OptEnvelopeDeprecatedRow EnvelopeType = `deprecated_row`
	OptEnvelopeWrapped       EnvelopeType = `wrapped`

	OptFormatJSON FormatType = `json`
	OptFormatAvro FormatType = `experimental_avro`

	SinkParamCACert           = `ca_cert`
	SinkParamClientCert       = `client_cert`
	SinkParamClientKey        = `client_key`
	SinkParamFileSize         = `file_size`
	SinkParamSchemaTopic      = `schema_topic`
	SinkParamTLSEnabled       = `tls_enabled`
	SinkParamTopicPrefix      = `topic_prefix`
	SinkSchemeBuffer          = ``
	SinkSchemeExperimentalSQL = `experimental-sql`
	SinkSchemeKafka           = `kafka`
	SinkParamSASLEnabled      = `sasl_enabled`
	SinkParamSASLHandshake    = `sasl_handshake`
	SinkParamSASLUser         = `sasl_user`
	SinkParamSASLPassword     = `sasl_password`
)

// ChangefeedOptionExpectValues is used to parse changefeed options using
// PlanHookState.TypeAsStringOpts().
var ChangefeedOptionExpectValues = map[string]sql.KVStringOptValidate{
	OptConfluentSchemaRegistry:  sql.KVStringOptRequireValue,
	OptCursor:                   sql.KVStringOptRequireValue,
	OptEnvelope:                 sql.KVStringOptRequireValue,
	OptFormat:                   sql.KVStringOptRequireValue,
	OptKeyInValue:               sql.KVStringOptRequireNoValue,
	OptResolvedTimestamps:       sql.KVStringOptAny,
	OptUpdatedTimestamps:        sql.KVStringOptRequireNoValue,
	OptDiff:                     sql.KVStringOptRequireNoValue,
	OptCompression:              sql.KVStringOptRequireValue,
	OptSchemaChangeEvents:       sql.KVStringOptRequireValue,
	OptSchemaChangePolicy:       sql.KVStringOptRequireValue,
	OptInitialScan:              sql.KVStringOptRequireNoValue,
	OptNoInitialScan:            sql.KVStringOptRequireNoValue,
	OptProtectDataFromGCOnPause: sql.KVStringOptRequireNoValue,
}

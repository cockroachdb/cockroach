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

// Constants for the options.
const (
	OptConfluentSchemaRegistry = `confluent_schema_registry`
	OptCursor                  = `cursor`
	OptEnvelope                = `envelope`
	OptFormat                  = `format`
	OptKeyInValue              = `key_in_value`
	OptResolvedTimestamps      = `resolved`
	OptUpdatedTimestamps       = `updated`
	OptDiff                    = `diff`

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
	OptConfluentSchemaRegistry: sql.KVStringOptRequireValue,
	OptCursor:                  sql.KVStringOptRequireValue,
	OptEnvelope:                sql.KVStringOptRequireValue,
	OptFormat:                  sql.KVStringOptRequireValue,
	OptKeyInValue:              sql.KVStringOptRequireNoValue,
	OptResolvedTimestamps:      sql.KVStringOptAny,
	OptUpdatedTimestamps:       sql.KVStringOptRequireNoValue,
	OptDiff:                    sql.KVStringOptRequireNoValue,
}

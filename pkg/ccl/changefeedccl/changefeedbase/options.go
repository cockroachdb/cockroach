// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

// StatementOptions provides friendlier access to the options map from the WITH
// part of a changefeed statement and smaller bundles to pass around.
// Construct it by calling MakeStatementOptions on the raw options map.
// Where possible, it will error when retrieving an invalid value.
type StatementOptions struct {
	m map[string]string

	// TODO (zinger): Structs are created lazily in order to keep validations
	// and options munging in the same order.
	// Rework changefeed_stmt.go so that we can have one static StatementOptions
	// that validates everything at once and don't need this cache.
	cache struct {
		EncodingOptions
	}
}

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

// InitialScanType configures whether the changefeed will perform an
// initial scan, and the type of initial scan that it will perform
type InitialScanType int

// SinkSpecificJSONConfig is a JSON string that the sink is responsible
// for parsing, validating, and honoring.
type SinkSpecificJSONConfig string

// Constants for the initial scan types
const (
	InitialScan InitialScanType = iota
	NoInitialScan
	OnlyInitialScan
)

// Constants for the options.
const (
	OptAvroSchemaPrefix         = `avro_schema_prefix`
	OptConfluentSchemaRegistry  = `confluent_schema_registry`
	OptCursor                   = `cursor`
	OptEndTime                  = `end_time`
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

	OptInitialScanOnly = `initial_scan_only`

	OptEnvelopeKeyOnly       EnvelopeType = `key_only`
	OptEnvelopeRow           EnvelopeType = `row`
	OptEnvelopeDeprecatedRow EnvelopeType = `deprecated_row`
	OptEnvelopeWrapped       EnvelopeType = `wrapped`
	OptEnvelopeBare          EnvelopeType = `bare`

	OptFormatJSON    FormatType = `json`
	OptFormatAvro    FormatType = `avro`
	OptFormatCSV     FormatType = `csv`
	OptFormatParquet FormatType = `parquet`

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
	SinkSchemeExternalConnection    = `external`
	SinkParamSASLEnabled            = `sasl_enabled`
	SinkParamSASLHandshake          = `sasl_handshake`
	SinkParamSASLUser               = `sasl_user`
	SinkParamSASLPassword           = `sasl_password`
	SinkParamSASLMechanism          = `sasl_mechanism`

	RegistryParamCACert     = `ca_cert`
	RegistryParamClientCert = `client_cert`
	RegistryParamClientKey  = `client_key`

	// Topics is used to store the topics generated by the sink in the options
	// struct so that they can be displayed in the show changefeed jobs query.
	// Hence, this option is not available to users
	Topics = `topics`
)

func makeStringSet(opts ...string) map[string]struct{} {
	res := make(map[string]struct{}, len(opts))
	for _, opt := range opts {
		res[opt] = struct{}{}
	}
	return res
}

func unionStringSets(sets ...map[string]struct{}) map[string]struct{} {
	res := make(map[string]struct{})
	for _, s := range sets {
		for k := range s {
			res[k] = struct{}{}
		}
	}
	return res
}

// OptionType is an enum of the ways changefeed options can be provided in WITH.
type OptionType int

// Constants defining OptionTypes.
const (
	// OptionTypeString is a catch-all for options needing a value.
	OptionTypeString OptionType = iota

	OptionTypeTimestamp

	OptionTypeDuration

	// Boolean options set to true if present, false if absent.
	OptionTypeFlag

	OptionTypeEnum

	OptionTypeJSON
)

// OptionPermittedValues is used in validations and is meant to be self-documenting.
// TODO (zinger): Also use this in docgen.
type OptionPermittedValues struct {
	// Type is what this option will eventually be parsed as.
	Type OptionType

	// EnumValues lists all possible values for OptionTypeEnum.
	// Empty for non-enums.
	EnumValues map[string]struct{}

	// CanBeEmpty describes an option that can be provided either as a key with no value,
	// or a key/value pair.
	CanBeEmpty bool

	// CanBeEmpty describes an option for which an explicit '0' is allowed.
	CanBeZero bool

	// IfEmpty gives the semantic meaning of the empty form of a CanBeEmpty option.
	// Blank for other kinds of options. This is not the same as the default value.
	IfEmpty string

	desc string
}

func enum(strs ...string) OptionPermittedValues {
	return OptionPermittedValues{
		Type:       OptionTypeEnum,
		EnumValues: makeStringSet(strs...),
		desc:       describeEnum(strs...),
	}
}

func (o OptionPermittedValues) orEmptyMeans(def string) OptionPermittedValues {
	o2 := o
	o2.CanBeEmpty = true
	o2.IfEmpty = def
	return o2
}

func (o OptionPermittedValues) thatCanBeZero() OptionPermittedValues {
	o2 := o
	o2.CanBeZero = true
	return o2
}

var stringOption = OptionPermittedValues{Type: OptionTypeString}
var durationOption = OptionPermittedValues{Type: OptionTypeDuration}
var timestampOption = OptionPermittedValues{Type: OptionTypeTimestamp}
var flagOption = OptionPermittedValues{Type: OptionTypeFlag}
var jsonOption = OptionPermittedValues{Type: OptionTypeJSON}

// ChangefeedOptionExpectValues is used to parse changefeed options using
// PlanHookState.TypeAsStringOpts().
var ChangefeedOptionExpectValues = map[string]OptionPermittedValues{
	OptAvroSchemaPrefix:         stringOption,
	OptConfluentSchemaRegistry:  stringOption,
	OptCursor:                   timestampOption,
	OptEndTime:                  timestampOption,
	OptEnvelope:                 enum("row", "key_only", "wrapped", "deprecated_row", "bare"),
	OptFormat:                   enum("json", "avro", "csv", "experimental_avro", "parquet"),
	OptFullTableName:            flagOption,
	OptKeyInValue:               flagOption,
	OptTopicInValue:             flagOption,
	OptResolvedTimestamps:       durationOption.thatCanBeZero().orEmptyMeans("0"),
	OptMinCheckpointFrequency:   durationOption.thatCanBeZero(),
	OptUpdatedTimestamps:        flagOption,
	OptMVCCTimestamps:           flagOption,
	OptDiff:                     flagOption,
	OptCompression:              enum("gzip", "zstd"),
	OptSchemaChangeEvents:       enum("column_changes", "default"),
	OptSchemaChangePolicy:       enum("backfill", "nobackfill", "stop", "ignore"),
	OptSplitColumnFamilies:      flagOption,
	OptInitialScan:              enum("yes", "no", "only").orEmptyMeans("yes"),
	OptNoInitialScan:            flagOption,
	OptInitialScanOnly:          flagOption,
	OptProtectDataFromGCOnPause: flagOption,
	OptKafkaSinkConfig:          jsonOption,
	OptWebhookSinkConfig:        jsonOption,
	OptWebhookAuthHeader:        stringOption,
	OptWebhookClientTimeout:     durationOption,
	OptOnError:                  enum("pause", "fail"),
	OptMetricsScope:             stringOption,
	OptVirtualColumns:           enum("omitted", "null"),
}

// CommonOptions is options common to all sinks
var CommonOptions = makeStringSet(OptCursor, OptEndTime, OptEnvelope,
	OptFormat, OptFullTableName,
	OptKeyInValue, OptTopicInValue,
	OptResolvedTimestamps, OptUpdatedTimestamps,
	OptMVCCTimestamps, OptDiff, OptSplitColumnFamilies,
	OptSchemaChangeEvents, OptSchemaChangePolicy,
	OptProtectDataFromGCOnPause, OptOnError,
	OptInitialScan, OptNoInitialScan, OptInitialScanOnly,
	OptMinCheckpointFrequency, OptMetricsScope, OptVirtualColumns, Topics)

// SQLValidOptions is options exclusive to SQL sink
var SQLValidOptions map[string]struct{} = nil

// KafkaValidOptions is options exclusive to Kafka sink
var KafkaValidOptions = makeStringSet(OptAvroSchemaPrefix, OptConfluentSchemaRegistry, OptKafkaSinkConfig)

// CloudStorageValidOptions is options exclusive to cloud storage sink
var CloudStorageValidOptions = makeStringSet(OptCompression)

// WebhookValidOptions is options exclusive to webhook sink
var WebhookValidOptions = makeStringSet(OptWebhookAuthHeader, OptWebhookClientTimeout, OptWebhookSinkConfig)

// PubsubValidOptions is options exclusive to pubsub sink
var PubsubValidOptions = makeStringSet()

// ExternalConnectionValidOptions is options exclusive to the external
// connection sink.
//
// TODO(adityamaru): Some of these options should be supported when creating the
// external connection rather than when setting up the changefeed. Move them once
// we support `CREATE EXTERNAL CONNECTION ... WITH <options>`.
var ExternalConnectionValidOptions = unionStringSets(SQLValidOptions, KafkaValidOptions, CloudStorageValidOptions, WebhookValidOptions, PubsubValidOptions)

// CaseInsensitiveOpts options which supports case Insensitive value
var CaseInsensitiveOpts = makeStringSet(OptFormat, OptEnvelope, OptCompression, OptSchemaChangeEvents,
	OptSchemaChangePolicy, OptOnError, OptInitialScan)

// redactionFunc is a function applied to a string option which returns its redacted value.
type redactionFunc func(string) (string, error)

var redactSimple = func(string) (string, error) {
	return "redacted", nil
}

// RedactUserFromURI takes a URI string and removes the user from it.
// If there is no user, the original URI is returned.
func RedactUserFromURI(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	if u.User != nil {
		u.User = url.User(`redacted`)
	}
	return u.String(), nil
}

// RedactedOptions are options whose values should be replaced with "redacted" in job descriptions and errors.
var RedactedOptions = map[string]redactionFunc{
	OptWebhookAuthHeader:       redactSimple,
	SinkParamClientKey:         redactSimple,
	OptConfluentSchemaRegistry: RedactUserFromURI,
}

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

// InitialScanOnlyUnsupportedOptions is options that are not supported with the
// initial scan only option
var InitialScanOnlyUnsupportedOptions = makeStringSet(OptEndTime, OptResolvedTimestamps, OptDiff,
	OptMVCCTimestamps, OptUpdatedTimestamps)

// AlterChangefeedUnsupportedOptions are changefeed options that we do not allow
// users to alter.
// TODO(sherman): At the moment we disallow altering both the initial_scan_only
// and the end_time option. However, there are instances in which it should be
// allowed to alter either of these options. We need to support the alteration
// of these fields.
var AlterChangefeedUnsupportedOptions = makeStringSet(OptCursor, OptInitialScan,
	OptNoInitialScan, OptInitialScanOnly, OptEndTime)

// AlterChangefeedOptionExpectValues is used to parse alter changefeed options
// using PlanHookState.TypeAsStringOpts().
var AlterChangefeedOptionExpectValues = func() map[string]OptionPermittedValues {
	alterChangefeedOptions := make(map[string]OptionPermittedValues, len(ChangefeedOptionExpectValues)+1)
	for key, value := range ChangefeedOptionExpectValues {
		alterChangefeedOptions[key] = value
	}
	alterChangefeedOptions[OptSink] = stringOption
	return alterChangefeedOptions
}()

// AlterChangefeedTargetOptions is used to parse target specific alter
// changefeed options using PlanHookState.TypeAsStringOpts().
var AlterChangefeedTargetOptions = map[string]OptionPermittedValues{
	OptInitialScan:   enum("yes", "no", "only").orEmptyMeans("yes"),
	OptNoInitialScan: flagOption,
}

// MakeStatementOptions wraps and canonicalizes the options we get
// from TypeAsStringOpts or the job record.
func MakeStatementOptions(opts map[string]string) StatementOptions {
	if opts == nil {
		return MakeDefaultOptions()
	}
	mapCopy := make(map[string]string, len(opts))
	for key, value := range opts {
		if _, ok := CaseInsensitiveOpts[key]; ok {
			mapCopy[key] = strings.ToLower(value)
		} else {
			mapCopy[key] = value
		}
	}
	return StatementOptions{m: mapCopy}
}

// MakeDefaultOptions creates the StatementOptions you'd get from
// a changefeed statement with no WITH.
func MakeDefaultOptions() StatementOptions {
	return StatementOptions{m: make(map[string]string)}
}

// AsMap gets the untyped version of a StatementOptions we serialize
// in a jobspb.ChangefeedDetails. This can't be automagically cast
// without introducing a dependency.
func (s StatementOptions) AsMap() map[string]string {
	return s.m
}

// IsSet checks whether the given key was set explicitly.
func (s StatementOptions) IsSet(key string) bool {
	_, ok := s.m[key]
	return ok
}

// DeprecationWarnings checks for options in forms we still support and serialize,
// but should be replaced with a new form. Currently hardcoded to just check format.
func (s StatementOptions) DeprecationWarnings() []string {
	if newFormat, ok := NoLongerExperimental[s.m[OptFormat]]; ok {
		return []string{fmt.Sprintf(`%[1]s is no longer experimental, use %[2]s=%[1]s`,
			newFormat, OptFormat)}
	}

	return []string{}
}

// ForEachWithRedaction iterates a function over the raw key/value pairs.
// Meant for serialization.
func (s StatementOptions) ForEachWithRedaction(fn func(k string, v string)) error {
	for k, v := range s.m {
		if redactionFunc, redact := RedactedOptions[k]; redact {
			redactedVal, err := redactionFunc(v)
			if err != nil {
				return err
			}
			fn(k, redactedVal)
		} else {
			fn(k, v)
		}
	}
	return nil
}

// HasStartCursor returns true if we're starting from a
// user-provided timestamp.
func (s StatementOptions) HasStartCursor() bool {
	_, ok := s.m[OptCursor]
	return ok
}

// GetCursor returns the user-provided cursor.
func (s StatementOptions) GetCursor() string {
	return s.m[OptCursor]
}

// HasEndTime returns true if an end time was provided.
func (s StatementOptions) HasEndTime() bool {
	_, ok := s.m[OptEndTime]
	return ok
}

// GetEndTime returns the user-provided end time.
func (s StatementOptions) GetEndTime() string {
	return s.m[OptEndTime]
}

func (s StatementOptions) getEnumValue(k string) (string, error) {
	enumOptions := ChangefeedOptionExpectValues[k]
	rawVal, present := s.m[k]
	if !present {
		return ``, nil
	}
	if rawVal == `` {
		return enumOptions.IfEmpty, nil
	}

	if _, ok := enumOptions.EnumValues[rawVal]; !ok {
		return ``, errors.Errorf(
			`unknown %s: %s, %s`, k, rawVal, enumOptions.desc)
	}

	return rawVal, nil
}

func (s StatementOptions) getDurationValue(k string) (*time.Duration, error) {
	v, ok := s.m[k]
	if !ok {
		return nil, nil
	}
	if v == `` {
		v = ChangefeedOptionExpectValues[k].IfEmpty
	}
	if d, err := time.ParseDuration(v); err != nil {
		return nil, errors.Wrapf(err, "problem parsing option %s", k)
	} else if d < 0 {
		return nil, errors.Errorf("negative durations are not accepted: %s='%s'", k, v)
	} else if d == 0 && !ChangefeedOptionExpectValues[k].CanBeZero {
		return nil, errors.Errorf("option %s must be a duration greater than 0", k)
	} else {
		return &d, nil
	}
}

func (s StatementOptions) getJSONValue(k string) SinkSpecificJSONConfig {
	return SinkSpecificJSONConfig(s.m[k])
}

// GetInitialScanType determines the type of initial scan the changefeed
// should perform on the first run given the options provided from the user.
func (s StatementOptions) GetInitialScanType() (InitialScanType, error) {
	_, initialScanSet := s.m[OptInitialScan]
	_, initialScanOnlySet := s.m[OptInitialScanOnly]
	_, noInitialScanSet := s.m[OptNoInitialScan]

	if initialScanSet && noInitialScanSet {
		return InitialScan, errors.Errorf(
			`cannot specify both %s and %s`, OptInitialScan,
			OptNoInitialScan)
	}

	if initialScanSet && initialScanOnlySet {
		return InitialScan, errors.Errorf(
			`cannot specify both %s and %s`, OptInitialScan,
			OptInitialScanOnly)
	}

	if noInitialScanSet && initialScanOnlySet {
		return InitialScan, errors.Errorf(
			`cannot specify both %s and %s`, OptInitialScanOnly,
			OptNoInitialScan)
	}

	if initialScanSet {
		const opt = OptInitialScan
		v, err := s.getEnumValue(opt)
		if err != nil {
			return InitialScan, err
		}
		switch v {
		case `yes`:
			return InitialScan, nil
		case `no`:
			return NoInitialScan, nil
		case `only`:
			return OnlyInitialScan, nil
		}
	}

	if initialScanOnlySet {
		return OnlyInitialScan, nil
	}

	if noInitialScanSet {
		return NoInitialScan, nil
	}

	// If we reach this point, this implies that the user did not specify any initial scan
	// options. In this case the default behaviour is to perform an initial scan if the
	// cursor is not specified.
	if !s.HasStartCursor() {
		return InitialScan, nil
	}

	return NoInitialScan, nil
}

func (s StatementOptions) IsInitialScanSpecified() bool {
	_, initialScanSet := s.m[OptInitialScan]
	_, initialScanOnlySet := s.m[OptInitialScanOnly]
	_, noInitialScanSet := s.m[OptNoInitialScan]

	if !initialScanSet && !initialScanOnlySet && !noInitialScanSet {
		return false
	}

	return true
}

// ShouldUseFullStatementTimeName returns true if references to the table should be in db.schema.table
// format (e.g. in Kafka topics).
func (s StatementOptions) ShouldUseFullStatementTimeName() bool {
	_, qualified := s.m[OptFullTableName]
	return qualified
}

// CanHandle tracks whether users have explicitly specificed how to handle
// unusual table schemas.
type CanHandle struct {
	MultipleColumnFamilies bool
	VirtualColumns         bool
}

// GetCanHandle returns a populated CanHandle.
func (s StatementOptions) GetCanHandle() CanHandle {
	_, families := s.m[OptSplitColumnFamilies]
	_, virtual := s.m[OptVirtualColumns]
	return CanHandle{
		MultipleColumnFamilies: families,
		VirtualColumns:         virtual,
	}
}

// EncodingOptions describe how events are encoded when
// sent to the sink.
type EncodingOptions struct {
	Format            FormatType
	VirtualColumns    VirtualColumnVisibility
	Envelope          EnvelopeType
	KeyInValue        bool
	TopicInValue      bool
	UpdatedTimestamps bool
	MVCCTimestamps    bool
	Diff              bool
	AvroSchemaPrefix  string
	SchemaRegistryURI string
	Compression       string
}

// GetEncodingOptions populates and validates an EncodingOptions.
func (s StatementOptions) GetEncodingOptions() (EncodingOptions, error) {
	o := EncodingOptions{}
	if s.cache.EncodingOptions != o {
		return s.cache.EncodingOptions, nil
	}
	format, err := s.getEnumValue(OptFormat)
	if err != nil {
		return o, err
	}
	if format == `` {
		o.Format = OptFormatJSON
	} else {
		o.Format = FormatType(format)
	}
	virt, err := s.getEnumValue(OptVirtualColumns)
	if err != nil {
		return o, err
	}
	if virt == `` {
		o.VirtualColumns = OptVirtualColumnsOmitted
	} else {
		o.VirtualColumns = VirtualColumnVisibility(virt)
	}
	envelope, err := s.getEnumValue(OptEnvelope)
	if err != nil {
		return o, err
	}
	if envelope == `` {
		o.Envelope = OptEnvelopeWrapped
	} else {
		o.Envelope = EnvelopeType(envelope)
	}

	_, o.KeyInValue = s.m[OptKeyInValue]
	_, o.TopicInValue = s.m[OptTopicInValue]
	_, o.UpdatedTimestamps = s.m[OptUpdatedTimestamps]
	_, o.MVCCTimestamps = s.m[OptMVCCTimestamps]
	_, o.Diff = s.m[OptDiff]

	o.SchemaRegistryURI = s.m[OptConfluentSchemaRegistry]
	o.AvroSchemaPrefix = s.m[OptAvroSchemaPrefix]
	o.Compression = s.m[OptCompression]

	s.cache.EncodingOptions = o
	return o, o.Validate()
}

// Validate checks for incompatible encoding options.
func (e EncodingOptions) Validate() error {
	if e.Envelope == OptEnvelopeRow && e.Format == OptFormatAvro {
		return errors.Errorf(`%s=%s is not supported with %s=%s`,
			OptEnvelope, OptEnvelopeRow, OptFormat, OptFormatAvro,
		)
	}
	if e.Envelope != OptEnvelopeWrapped && e.Format != OptFormatJSON && e.Format != OptFormatParquet {
		requiresWrap := []struct {
			k string
			b bool
		}{
			{OptKeyInValue, e.KeyInValue},
			{OptTopicInValue, e.TopicInValue},
			{OptUpdatedTimestamps, e.UpdatedTimestamps},
			{OptMVCCTimestamps, e.MVCCTimestamps},
			{OptDiff, e.Diff},
		}
		for _, v := range requiresWrap {
			if v.b {
				return errors.Errorf(`%s is only usable with %s=%s`,
					v.k, OptEnvelope, OptEnvelopeWrapped)
			}
		}
	}
	return nil
}

// SchemaChangeHandlingOptions specify how the feed should
// behave when a target is affected by a schema change.
type SchemaChangeHandlingOptions struct {
	EventClass SchemaChangeEventClass
	Policy     SchemaChangePolicy
}

// GetSchemaChangeHandlingOptions populates and validates a SchemaChangeHandlingOptions.
func (s StatementOptions) GetSchemaChangeHandlingOptions() (SchemaChangeHandlingOptions, error) {
	o := SchemaChangeHandlingOptions{}
	ec, err := s.getEnumValue(OptSchemaChangeEvents)
	if err != nil {
		return o, err
	}
	if ec == `` {
		o.EventClass = OptSchemaChangeEventClassDefault
	} else {
		o.EventClass = SchemaChangeEventClass(ec)
	}

	p, err := s.getEnumValue(OptSchemaChangePolicy)
	if err != nil {
		return o, err
	}
	if p == `` {
		o.Policy = OptSchemaChangePolicyBackfill
	} else {
		o.Policy = SchemaChangePolicy(p)
	}

	return o, nil

}

// Filters are aspects of the feed that the backing
// kvfeed or rangefeed want to know about.
type Filters struct {
	WithDiff bool
}

// GetFilters returns a populated Filters.
func (s StatementOptions) GetFilters() Filters {
	_, withDiff := s.m[OptDiff]
	return Filters{
		WithDiff: withDiff,
	}
}

// WebhookSinkOptions are passed in WITH args but
// are specific to the webhook sink.
// ClientTimeout is nil if not set as the default
// is different from 0.
type WebhookSinkOptions struct {
	JSONConfig    SinkSpecificJSONConfig
	AuthHeader    string
	ClientTimeout *time.Duration
}

// GetWebhookSinkOptions includes arbitrary json to be interpreted
// by the webhook sink.
func (s StatementOptions) GetWebhookSinkOptions() (WebhookSinkOptions, error) {
	o := WebhookSinkOptions{JSONConfig: s.getJSONValue(OptWebhookSinkConfig), AuthHeader: s.m[OptWebhookAuthHeader]}
	timeout, err := s.getDurationValue(OptWebhookClientTimeout)
	if err != nil {
		return o, err
	}
	o.ClientTimeout = timeout
	return o, nil
}

// GetKafkaConfigJSON returns arbitrary json to be interpreted
// by the kafka sink.
func (s StatementOptions) GetKafkaConfigJSON() SinkSpecificJSONConfig {
	return s.getJSONValue(OptKafkaSinkConfig)
}

// GetResolvedTimestampInterval gets the best-effort interval at which resolved timestamps
// should be emitted. Nil or 0 means emit as often as possible. False means do not emit at all.
// Returns an error for negative or invalid duration value.
func (s StatementOptions) GetResolvedTimestampInterval() (*time.Duration, bool, error) {
	str, ok := s.m[OptResolvedTimestamps]
	if ok && str == OptEmitAllResolvedTimestamps {
		return nil, true, nil
	}
	d, err := s.getDurationValue(OptResolvedTimestamps)
	return d, d != nil, err
}

// GetMetricScope returns a namespace for metrics affected by this changefeed, or
// false if none has been provided.
func (s StatementOptions) GetMetricScope() (string, bool) {
	v, ok := s.m[OptMetricsScope]
	return v, ok
}

// IncludeVirtual returns true if we need to set placeholder nulls for virtual columns.
func (s StatementOptions) IncludeVirtual() bool {
	return s.m[OptVirtualColumns] == string(OptVirtualColumnsNull)
}

// KeyOnly returns true if we are using the 'key_only' envelope.
func (s StatementOptions) KeyOnly() bool {
	return s.m[OptEnvelope] == string(OptEnvelopeKeyOnly)
}

// GetMinCheckpointFrequency returns the minimum frequency with which checkpoints should be
// recorded. Returns nil if not set, and an error if invalid.
func (s StatementOptions) GetMinCheckpointFrequency() (*time.Duration, error) {
	return s.getDurationValue(OptMinCheckpointFrequency)
}

// ForceKeyInValue sets the encoding option KeyInValue to true and then validates the
// resoluting encoding options.
func (s StatementOptions) ForceKeyInValue() error {
	s.m[OptKeyInValue] = ``
	s.cache.EncodingOptions = EncodingOptions{}
	_, err := s.GetEncodingOptions()
	return err
}

// ForceTopicInValue sets the encoding option TopicInValue to true and then validates the
// resoluting encoding options.
func (s StatementOptions) ForceTopicInValue() error {
	s.m[OptTopicInValue] = ``
	s.cache.EncodingOptions = EncodingOptions{}
	_, err := s.GetEncodingOptions()
	return err
}

// ForceDiff sets diff to true regardess of its previous value.
func (s StatementOptions) ForceDiff() {
	s.m[OptDiff] = ``
	s.cache.EncodingOptions = EncodingOptions{}
}

// SetTopics stashes the list of topics in the options as a handy place
// to serialize it.
// TODO: Have a separate metadata map on the details proto for things
// like this.
func (s StatementOptions) SetTopics(topics []string) {
	s.m[Topics] = strings.Join(topics, ",")
}

// ClearDiff clears diff option.
func (s StatementOptions) ClearDiff() {
	delete(s.m, OptDiff)
	s.cache.EncodingOptions = EncodingOptions{}
}

// SetDefaultEnvelope sets the envelope if not already set.
func (s StatementOptions) SetDefaultEnvelope(t EnvelopeType) {
	if _, ok := s.m[OptEnvelope]; !ok {
		s.m[OptEnvelope] = string(t)
		s.cache.EncodingOptions = EncodingOptions{}
	}
}

// GetOnError validates and returns the desired behavior when a non-retriable error is encountered.
func (s StatementOptions) GetOnError() (OnErrorType, error) {
	v, err := s.getEnumValue(OptOnError)
	if err != nil || v == `` {
		return OptOnErrorFail, err
	}
	return OnErrorType(v), nil
}

func describeEnum(strs ...string) string {
	switch len(strs) {
	case 1:
		return fmt.Sprintf("the only valid value is '%s'", strs[0])
	case 2:
		return fmt.Sprintf("valid values are '%s' and '%s'", strs[0], strs[1])
	default:
		s := "valid values are "
		for i, v := range strs {
			if i > 0 {
				s = s + ", "
			}
			if i == len(strs)-1 {
				s = s + " and "
			}
			s = s + fmt.Sprintf("'%s'", v)
		}
		return s
	}
}

// ValidateForCreateChangefeed checks that the provided options are
// valid for a CREATE CHANGEFEED statement using the type assertions
// in ChangefeedOptionExpectValues.
func (s StatementOptions) ValidateForCreateChangefeed(isPredicateChangefeed bool) error {
	err := s.validateAgainst(ChangefeedOptionExpectValues)
	if err != nil {
		return err
	}
	scanType, err := s.GetInitialScanType()
	if err != nil {
		return err
	}
	validateInitialScanUnsupportedOptions := func(errMsg string) error {
		for o := range InitialScanOnlyUnsupportedOptions {
			if _, ok := s.m[o]; ok {
				return errors.Newf(`cannot specify both %s and %s`, errMsg, o)
			}
		}
		return nil
	}
	if scanType == OnlyInitialScan {
		if err := validateInitialScanUnsupportedOptions(OptInitialScanOnly); err != nil {
			return err
		}
	} else {
		if s.m[OptFormat] == string(OptFormatCSV) {
			return errors.Newf(`%s=%s is only usable with %s`, OptFormat, OptFormatCSV, OptInitialScanOnly)
		}
	}
	// Right now parquet does not support any of these options
	if s.m[OptFormat] == string(OptFormatParquet) {
		if isPredicateChangefeed {
			// Diff option is allowed when using predicate changefeeds with parquet format.
			for o := range InitialScanOnlyUnsupportedOptions {
				if _, ok := s.m[o]; ok && o != OptDiff {
					return errors.Newf(`cannot specify both format='%s' and %s`, OptFormatParquet, o)
				}
			}
		} else {
			if err := validateInitialScanUnsupportedOptions(string(OptFormatParquet)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s StatementOptions) validateAgainst(m map[string]OptionPermittedValues) error {
	for k := range ChangefeedOptionExpectValues {
		permitted := m[k]
		switch permitted.Type {
		case OptionTypeFlag:
			// No value to validate
		case OptionTypeString, OptionTypeTimestamp, OptionTypeJSON:
			// Consumer (usually a sink) must parse and validate these
		case OptionTypeDuration:
			if _, err := s.getDurationValue(k); err != nil {
				return err
			}
		case OptionTypeEnum:
			if _, err := s.getEnumValue(k); err != nil {
				return err
			}
		}
	}
	return nil
}

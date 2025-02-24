// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
		*EncodingOptions
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

// EnrichedProperty is used with the `enriched_properties` option to specify
// which properties are included in the enriched envelope. That option is specified
// as a csv of these values.
type EnrichedProperty string

const (
	EnrichedPropertySource EnrichedProperty = `source`
	EnrichedPropertySchema EnrichedProperty = `schema`
)

// Constants for the initial scan types
const (
	InitialScan InitialScanType = iota
	NoInitialScan
	OnlyInitialScan
)

// Constants for the options.
const (
	OptAvroSchemaPrefix                   = `avro_schema_prefix`
	OptConfluentSchemaRegistry            = `confluent_schema_registry`
	OptCursor                             = `cursor`
	OptCustomKeyColumn                    = `key_column`
	OptEndTime                            = `end_time`
	OptEnvelope                           = `envelope`
	OptFormat                             = `format`
	OptFullTableName                      = `full_table_name`
	OptKeyInValue                         = `key_in_value`
	OptTopicInValue                       = `topic_in_value`
	OptResolvedTimestamps                 = `resolved`
	OptMinCheckpointFrequency             = `min_checkpoint_frequency`
	OptUpdatedTimestamps                  = `updated`
	OptMVCCTimestamps                     = `mvcc_timestamp`
	OptDiff                               = `diff`
	OptCompression                        = `compression`
	OptSchemaChangeEvents                 = `schema_change_events`
	OptSchemaChangePolicy                 = `schema_change_policy`
	OptSplitColumnFamilies                = `split_column_families`
	OptExpirePTSAfter                     = `gc_protect_expires_after`
	OptWebhookAuthHeader                  = `webhook_auth_header`
	OptWebhookClientTimeout               = `webhook_client_timeout`
	OptOnError                            = `on_error`
	OptMetricsScope                       = `metrics_label`
	OptUnordered                          = `unordered`
	OptVirtualColumns                     = `virtual_columns`
	OptExecutionLocality                  = `execution_locality`
	OptLaggingRangesThreshold             = `lagging_ranges_threshold`
	OptLaggingRangesPollingInterval       = `lagging_ranges_polling_interval`
	OptIgnoreDisableChangefeedReplication = `ignore_disable_changefeed_replication`
	OptEncodeJSONValueNullAsObject        = `encode_json_value_null_as_object`

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
	// OptNoInitialScan disables an initial scan. This is the default when a
	// cursor is specified. This option is useful to create a changefeed which
	// subscribes only to new messages.
	OptNoInitialScan = `no_initial_scan`
	// OptEmitAllResolvedTimestamps is a sentinel value to indicate that all
	// resolved timestamp events should be emitted.
	OptEmitAllResolvedTimestamps = ``

	OptInitialScanOnly = `initial_scan_only`

	OptEnrichedProperties = `enriched_properties`

	OptEnvelopeKeyOnly       EnvelopeType = `key_only`
	OptEnvelopeRow           EnvelopeType = `row`
	OptEnvelopeDeprecatedRow EnvelopeType = `deprecated_row`
	OptEnvelopeWrapped       EnvelopeType = `wrapped`
	OptEnvelopeBare          EnvelopeType = `bare`
	OptEnvelopeEnriched      EnvelopeType = `enriched`

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

	// DeprecatedSinkSchemeHTTP is interpreted as cloudstorage over HTTP PUT.
	DeprecatedSinkSchemeHTTP = `http`
	// DeprecatedSinkSchemeHTTPS is interpreted as cloudstorage over HTTPS PUT.
	DeprecatedSinkSchemeHTTPS = `https`

	// OptKafkaSinkConfig is a JSON configuration for kafka sink (kafkaSinkConfig).
	OptKafkaSinkConfig   = `kafka_sink_config`
	OptPubsubSinkConfig  = `pubsub_sink_config`
	OptWebhookSinkConfig = `webhook_sink_config`

	// OptSink allows users to alter the Sink URI of an existing changefeed.
	// Note that this option is only allowed for alter changefeed statements.
	OptSink = `sink`

	// Deprecated options.
	DeprecatedOptProtectDataFromGCOnPause = `protect_data_from_gc_on_pause`

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
	SinkSchemeCloudStorageHTTP      = `file-http`
	SinkSchemeCloudStorageHTTPS     = `file-https`
	SinkSchemeCloudStorageNodelocal = `nodelocal`
	SinkSchemeCloudStorageS3        = `s3`
	SinkSchemeExperimentalSQL       = `experimental-sql`
	SinkSchemeKafka                 = `kafka`
	SinkSchemeNull                  = `null`
	SinkSchemeWebhookHTTP           = `webhook-http`
	SinkSchemeWebhookHTTPS          = `webhook-https`
	SinkSchemePulsar                = `pulsar`
	SinkSchemeExternalConnection    = `external`
	SinkParamSASLEnabled            = `sasl_enabled`
	SinkParamSASLHandshake          = `sasl_handshake`
	SinkParamSASLUser               = `sasl_user`
	SinkParamSASLPassword           = `sasl_password`
	SinkParamSASLMechanism          = `sasl_mechanism`
	SinkParamSASLClientID           = `sasl_client_id`
	SinkParamSASLClientSecret       = `sasl_client_secret`
	SinkParamSASLTokenURL           = `sasl_token_url`
	SinkParamSASLScopes             = `sasl_scopes`
	SinkParamSASLGrantType          = `sasl_grant_type`
	SinkParamSASLAwsIAMRoleArn      = `sasl_aws_iam_role_arn`
	SinkParamSASLAwsRegion          = `sasl_aws_region`
	SinkParamSASLAwsIAMSessionName  = `sasl_aws_iam_session_name`
	SinkParamTableNameAttribute     = `with_table_name_attribute`

	// These are custom fields required for proprietary oauth. They should not
	// be documented.
	SinkParamSASLProprietaryResource            = `sasl_proprietary_resource`
	SinkParamSASLProprietaryClientAssertionType = `sasl_proprietary_client_assertion_type`
	SinkParamSASLProprietaryClientAssertion     = `sasl_proprietary_client_assertion`

	SinkSchemeConfluentKafka    = `confluent-cloud`
	SinkParamConfluentAPIKey    = `api_key`
	SinkParamConfluentAPISecret = `api_secret`

	SinkSchemeAzureKafka        = `azure-event-hub`
	SinkParamAzureAccessKeyName = `shared_access_key_name`
	SinkParamAzureAccessKey     = `shared_access_key`

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

	OptionTypeCommaSepStrings
)

// OptionPermittedValues is used in validations and is meant to be self-documenting.
// TODO (zinger): Also use this in docgen.
type OptionPermittedValues struct {
	// Type is what this option will eventually be parsed as.
	Type OptionType

	// EnumValues lists all possible values for OptionTypeEnum.
	// Empty for non-enums.
	EnumValues map[string]struct{}

	// CSVValues lists all possible values for OptionTypeCommaSepStrings.
	// Empty for non-CSVs.
	CSVValues map[string]struct{}

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

func csv(strs ...string) OptionPermittedValues {
	return OptionPermittedValues{
		Type:      OptionTypeCommaSepStrings,
		CSVValues: makeStringSet(strs...),
		desc:      describeCSV(strs...),
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
	OptAvroSchemaPrefix:                   stringOption,
	OptConfluentSchemaRegistry:            stringOption,
	OptCursor:                             timestampOption,
	OptCustomKeyColumn:                    stringOption,
	OptEndTime:                            timestampOption,
	OptEnvelope:                           enum("row", "key_only", "wrapped", "deprecated_row", "bare", "enriched"),
	OptFormat:                             enum("json", "avro", "csv", "experimental_avro", "parquet"),
	OptFullTableName:                      flagOption,
	OptKeyInValue:                         flagOption,
	OptTopicInValue:                       flagOption,
	OptResolvedTimestamps:                 durationOption.thatCanBeZero().orEmptyMeans("0"),
	OptMinCheckpointFrequency:             durationOption.thatCanBeZero(),
	OptUpdatedTimestamps:                  flagOption,
	OptMVCCTimestamps:                     flagOption,
	OptDiff:                               flagOption,
	OptCompression:                        enum("gzip", "zstd"),
	OptSchemaChangeEvents:                 enum("column_changes", "default"),
	OptSchemaChangePolicy:                 enum("backfill", "nobackfill", "stop", "ignore"),
	OptSplitColumnFamilies:                flagOption,
	OptInitialScan:                        enum("yes", "no", "only").orEmptyMeans("yes"),
	OptNoInitialScan:                      flagOption,
	OptInitialScanOnly:                    flagOption,
	DeprecatedOptProtectDataFromGCOnPause: flagOption,
	OptExpirePTSAfter:                     durationOption.thatCanBeZero(),
	OptKafkaSinkConfig:                    jsonOption,
	OptPubsubSinkConfig:                   jsonOption,
	OptWebhookSinkConfig:                  jsonOption,
	OptWebhookAuthHeader:                  stringOption,
	OptWebhookClientTimeout:               durationOption,
	OptOnError:                            enum("pause", "fail"),
	OptMetricsScope:                       stringOption,
	OptUnordered:                          flagOption,
	OptVirtualColumns:                     enum("omitted", "null"),
	OptExecutionLocality:                  stringOption,
	OptLaggingRangesThreshold:             durationOption,
	OptLaggingRangesPollingInterval:       durationOption,
	OptIgnoreDisableChangefeedReplication: flagOption,
	OptEncodeJSONValueNullAsObject:        flagOption,
	OptEnrichedProperties:                 csv(string(EnrichedPropertySource), string(EnrichedPropertySchema)),
}

// CommonOptions is options common to all sinks
var CommonOptions = makeStringSet(OptCursor, OptEndTime, OptEnvelope,
	OptFormat, OptFullTableName,
	OptKeyInValue, OptTopicInValue,
	OptResolvedTimestamps, OptUpdatedTimestamps,
	OptMVCCTimestamps, OptDiff, OptSplitColumnFamilies,
	OptSchemaChangeEvents, OptSchemaChangePolicy,
	OptOnError,
	OptInitialScan, OptNoInitialScan, OptInitialScanOnly, OptUnordered, OptCustomKeyColumn,
	OptMinCheckpointFrequency, OptMetricsScope, OptVirtualColumns, Topics, OptExpirePTSAfter,
	OptExecutionLocality, OptLaggingRangesThreshold, OptLaggingRangesPollingInterval,
	OptIgnoreDisableChangefeedReplication, OptEncodeJSONValueNullAsObject, OptEnrichedProperties,
)

// SQLValidOptions is options exclusive to SQL sink
var SQLValidOptions map[string]struct{} = nil

// KafkaValidOptions is options exclusive to Kafka sink
var KafkaValidOptions = makeStringSet(OptAvroSchemaPrefix, OptConfluentSchemaRegistry, OptKafkaSinkConfig)

// CloudStorageValidOptions is options exclusive to cloud storage sink
var CloudStorageValidOptions = makeStringSet(OptCompression)

// WebhookValidOptions is options exclusive to webhook sink
var WebhookValidOptions = makeStringSet(OptWebhookAuthHeader, OptWebhookClientTimeout, OptWebhookSinkConfig, OptCompression)

// PubsubValidOptions is options exclusive to pubsub sink
var PubsubValidOptions = makeStringSet(OptPubsubSinkConfig)

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

// RetiredOptions are the options which are no longer active.
var RetiredOptions = makeStringSet(DeprecatedOptProtectDataFromGCOnPause)

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

// OptionsSet is a test of changefeed option strings.
type OptionsSet map[string]struct{}

// InitialScanOnlyUnsupportedOptions is options that are not supported with the
// initial scan only option
var InitialScanOnlyUnsupportedOptions OptionsSet = makeStringSet(OptEndTime, OptResolvedTimestamps, OptDiff,
	OptMVCCTimestamps, OptUpdatedTimestamps)

// ParquetFormatUnsupportedOptions is options that are not supported with the
// parquet format.
var ParquetFormatUnsupportedOptions OptionsSet = makeStringSet(OptTopicInValue)

// AlterChangefeedUnsupportedOptions are changefeed options that we do not allow
// users to alter.
// TODO(sherman): At the moment we disallow altering both the initial_scan_only
// and the end_time option. However, there are instances in which it should be
// allowed to alter either of these options. We need to support the alteration
// of these fields.
var AlterChangefeedUnsupportedOptions OptionsSet = makeStringSet(OptCursor, OptInitialScan,
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

type optionRelationship struct {
	opt1   string
	opt2   string
	reason string
}

// opt1 and opt2 cannot both be set.
type incompatibleOptions optionRelationship

// if opt1 is set, opt2 must also be set.
type dependentOption optionRelationship

func makeInvertedIndex(pairs []incompatibleOptions) map[string][]incompatibleOptions {
	m := make(map[string][]incompatibleOptions, len(pairs)*2)
	for _, p := range pairs {
		m[p.opt1] = append(m[p.opt1], p)
		m[p.opt2] = append(m[p.opt2], p)
	}
	return m
}

func makeDirectedInvertedIndex(pairs []dependentOption) map[string][]dependentOption {
	m := make(map[string][]dependentOption, len(pairs))
	for _, p := range pairs {
		m[p.opt1] = append(m[p.opt1], p)
	}
	return m
}

var incompatibleOptionsMap = makeInvertedIndex([]incompatibleOptions{
	{opt1: OptUnordered, opt2: OptResolvedTimestamps, reason: `resolved timestamps cannot be guaranteed to be correct in unordered mode`},
})

var dependentOptionsMap = makeDirectedInvertedIndex([]dependentOption{
	{opt1: OptCustomKeyColumn, opt2: OptUnordered, reason: `using a value other than the primary key as the message key means end-to-end ordering cannot be preserved`},
})

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
// but should be replaced with a new form.
func (s StatementOptions) DeprecationWarnings() []string {
	if newFormat, ok := NoLongerExperimental[s.m[OptFormat]]; ok {
		return []string{fmt.Sprintf(`%[1]s is no longer experimental, use %[2]s=%[1]s`,
			newFormat, OptFormat)}
	}
	for retiredOpt := range RetiredOptions {
		if _, isSet := s.m[retiredOpt]; isSet {
			return []string{fmt.Sprintf("%s option is no longer needed", retiredOpt)}
		}
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

func (s StatementOptions) getCSVValues(k string) (map[string]struct{}, error) {
	permitted := ChangefeedOptionExpectValues[k]
	rawVal, present := s.m[k]
	if !present {
		return nil, nil
	}
	if rawVal == `` {
		return nil, nil
	}

	vals := strings.Split(rawVal, `,`)
	set := make(map[string]struct{}, len(vals))
	for _, val := range vals {
		val = strings.TrimSpace(val)
		if _, ok := permitted.CSVValues[val]; !ok {
			return nil, errors.Errorf(
				`unknown %s: %s, %s`, k, val, permitted.desc)
		}
		set[val] = struct{}{}
	}

	return set, nil
}

// getDurationValue validates that the option `k` was supplied with a
// valid duration.
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
			`cannot specify both %s='only' and %s`, OptInitialScan,
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
	RequiredColumns        []string
}

// GetCanHandle returns a populated CanHandle.
func (s StatementOptions) GetCanHandle() CanHandle {
	_, families := s.m[OptSplitColumnFamilies]
	_, virtual := s.m[OptVirtualColumns]
	h := CanHandle{
		MultipleColumnFamilies: families,
		VirtualColumns:         virtual,
	}
	if s.IsSet(OptCustomKeyColumn) {
		h.RequiredColumns = append(h.RequiredColumns, s.m[OptCustomKeyColumn])
	}
	return h
}

// EncodingOptions describe how events are encoded when
// sent to the sink.
type EncodingOptions struct {
	Format                      FormatType
	VirtualColumns              VirtualColumnVisibility
	Envelope                    EnvelopeType
	KeyInValue                  bool
	TopicInValue                bool
	UpdatedTimestamps           bool
	MVCCTimestamps              bool
	Diff                        bool
	EncodeJSONValueNullAsObject bool
	AvroSchemaPrefix            string
	SchemaRegistryURI           string
	Compression                 string
	CustomKeyColumn             string
	EnrichedProperties          map[EnrichedProperty]struct{}
}

// GetEncodingOptions populates and validates an EncodingOptions.
func (s StatementOptions) GetEncodingOptions() (EncodingOptions, error) {
	o := EncodingOptions{}
	if s.cache.EncodingOptions != nil {
		return *s.cache.EncodingOptions, nil
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
	_, o.EncodeJSONValueNullAsObject = s.m[OptEncodeJSONValueNullAsObject]

	o.SchemaRegistryURI = s.m[OptConfluentSchemaRegistry]
	o.AvroSchemaPrefix = s.m[OptAvroSchemaPrefix]
	o.Compression = s.m[OptCompression]
	o.CustomKeyColumn = s.m[OptCustomKeyColumn]

	enrichedProperties, err := s.getCSVValues(OptEnrichedProperties)
	if err != nil {
		return o, err
	}
	if len(enrichedProperties) > 0 {
		o.EnrichedProperties = make(map[EnrichedProperty]struct{}, len(enrichedProperties))
		for k := range enrichedProperties {
			o.EnrichedProperties[EnrichedProperty(k)] = struct{}{}
		}
	}

	s.cache.EncodingOptions = &o

	return o, o.Validate()
}

// Validate checks for incompatible encoding options.
func (e EncodingOptions) Validate() error {
	if e.Envelope == OptEnvelopeRow && e.Format == OptFormatAvro {
		return errors.Errorf(`%s=%s is not supported with %s=%s`,
			OptEnvelope, OptEnvelopeRow, OptFormat, OptFormatAvro,
		)
	}
	if e.Format != OptFormatJSON && e.EncodeJSONValueNullAsObject {
		return errors.Errorf(`%s is only usable with %s=%s`, OptEncodeJSONValueNullAsObject, OptFormat, OptFormatJSON)
	}

	if e.Envelope == OptEnvelopeEnriched {
		if e.Format != OptFormatJSON && e.Format != OptFormatAvro {
			return errors.Errorf(`%s=%s is only usable with %s=%s/%s`, OptEnvelope, OptEnvelopeEnriched, OptFormat, OptFormatJSON, OptFormatAvro)
		}
	} else {
		if len(e.EnrichedProperties) > 0 {
			return errors.Errorf(`%s is only usable with %s=%s`, OptEnrichedProperties, OptEnvelope, OptEnvelopeEnriched)
		}
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
	WithDiff      bool
	WithFiltering bool
}

// GetFilters returns a populated Filters.
func (s StatementOptions) GetFilters() Filters {
	_, withDiff := s.m[OptDiff]
	_, withIgnoreDisableChangefeedReplication := s.m[OptIgnoreDisableChangefeedReplication]
	return Filters{
		WithDiff:      withDiff,
		WithFiltering: !withIgnoreDisableChangefeedReplication,
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
	Compression   string
}

// GetWebhookSinkOptions includes arbitrary json to be interpreted
// by the webhook sink.
func (s StatementOptions) GetWebhookSinkOptions() (WebhookSinkOptions, error) {
	o := WebhookSinkOptions{
		JSONConfig:  s.getJSONValue(OptWebhookSinkConfig),
		AuthHeader:  s.m[OptWebhookAuthHeader],
		Compression: s.m[OptCompression],
	}
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

// GetPubsubConfigJSON returns arbitrary json to be interpreted
// by the pubsub sink.
func (s StatementOptions) GetPubsubConfigJSON() SinkSpecificJSONConfig {
	return s.getJSONValue(OptPubsubSinkConfig)
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

// GetLaggingRangesConfig returns the threshold and polling rate to use for
// lagging ranges metrics.
func (s StatementOptions) GetLaggingRangesConfig(
	ctx context.Context, settings *cluster.Settings,
) (threshold time.Duration, pollingInterval time.Duration, e error) {
	threshold = DefaultLaggingRangesThreshold
	pollingInterval = DefaultLaggingRangesPollingInterval
	_, ok := s.m[OptLaggingRangesThreshold]
	if ok {
		t, err := s.getDurationValue(OptLaggingRangesThreshold)
		if err != nil {
			return threshold, pollingInterval, err
		}
		threshold = *t
	}
	_, ok = s.m[OptLaggingRangesPollingInterval]
	if ok {
		i, err := s.getDurationValue(OptLaggingRangesPollingInterval)
		if err != nil {
			return threshold, pollingInterval, err
		}
		pollingInterval = *i
	}
	return threshold, pollingInterval, nil
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

func (s StatementOptions) GetConfluentSchemaRegistry() string {
	return s.m[OptConfluentSchemaRegistry]
}

// GetPTSExpiration returns the maximum age of the protected timestamp record.
// Changefeeds that fail to update their records in time will be canceled.
func (s StatementOptions) GetPTSExpiration() (time.Duration, error) {
	exp, err := s.getDurationValue(OptExpirePTSAfter)
	if err != nil {
		return 0, err
	}
	if exp == nil {
		return 0, nil
	}
	return *exp, nil
}

// ForceKeyInValue sets the encoding option KeyInValue to true and then validates the
// resoluting encoding options.
func (s StatementOptions) ForceKeyInValue() error {
	s.m[OptKeyInValue] = ``
	s.cache.EncodingOptions = &EncodingOptions{}
	_, err := s.GetEncodingOptions()
	return err
}

// ForceTopicInValue sets the encoding option TopicInValue to true and then validates the
// resoluting encoding options.
func (s StatementOptions) ForceTopicInValue() error {
	s.m[OptTopicInValue] = ``
	s.cache.EncodingOptions = &EncodingOptions{}
	_, err := s.GetEncodingOptions()
	return err
}

// ForceDiff sets diff to true regardess of its previous value.
func (s StatementOptions) ForceDiff() {
	s.m[OptDiff] = ``
	s.cache.EncodingOptions = &EncodingOptions{}
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
	s.cache.EncodingOptions = &EncodingOptions{}
}

// SetDefaultEnvelope sets the envelope if not already set.
func (s StatementOptions) SetDefaultEnvelope(t EnvelopeType) {
	if _, ok := s.m[OptEnvelope]; !ok {
		s.m[OptEnvelope] = string(t)
		s.cache.EncodingOptions = &EncodingOptions{}
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

func describeCSV(strs ...string) string {
	return fmt.Sprintf("valid values are: %s", strings.Join(strs, ", "))
}

// ValidateForCreateChangefeed checks that the provided options are
// valid for a CREATE CHANGEFEED statement using the type assertions
// in ChangefeedOptionExpectValues.
func (s StatementOptions) ValidateForCreateChangefeed(isPredicateChangefeed bool) (e error) {
	defer func() {
		e = pgerror.Wrap(e, pgcode.InvalidParameterValue, "")
	}()
	err := s.validateAgainst(ChangefeedOptionExpectValues)
	if err != nil {
		return err
	}
	scanType, err := s.GetInitialScanType()
	if err != nil {
		return err
	}

	// validateUnsupportedOptions returns an error if any of the supplied are
	// in the statement options. The error string should be the string
	// representation of the option (ex. "key_in_value", or "initial_scan='only'").
	validateUnsupportedOptions := func(unsupportedOptions OptionsSet, errorStr string) error {
		for o := range unsupportedOptions {
			if _, ok := s.m[o]; ok {
				return errors.Newf(`cannot specify both %s and %s`, errorStr, o)
			}
		}
		return nil
	}
	if scanType == OnlyInitialScan {
		if err := validateUnsupportedOptions(InitialScanOnlyUnsupportedOptions,
			fmt.Sprintf("%s='only'", OptInitialScan)); err != nil {
			return err
		}
	} else {
		if s.m[OptFormat] == string(OptFormatCSV) {
			return errors.Newf(`%s=%s is only usable with %s`, OptFormat, OptFormatCSV, OptInitialScanOnly)
		}
	}
	// Right now parquet does not support any of these options
	if s.m[OptFormat] == string(OptFormatParquet) {
		if err := validateUnsupportedOptions(ParquetFormatUnsupportedOptions, fmt.Sprintf("format=%s", OptFormatParquet)); err != nil {
			return err
		}
	}
	for o := range s.m {
		for _, pair := range incompatibleOptionsMap[o] {
			if s.IsSet(pair.opt1) && s.IsSet(pair.opt2) {
				return errors.Newf(`%s is not usable with %s because %s`, pair.opt1, pair.opt2, pair.reason)
			}
		}
	}
	for o := range s.m {
		for _, pair := range dependentOptionsMap[o] {
			if s.IsSet(pair.opt1) && !s.IsSet(pair.opt2) {
				return errors.Newf(`%s requires the %s option because %s`, pair.opt1, pair.opt2, pair.reason)
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
		case OptionTypeCommaSepStrings:
			if _, err := s.getCSVValues(k); err != nil {
				return err
			}
		}
	}
	return nil
}

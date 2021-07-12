// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	gosql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// TopicDescriptor describes topic emitted by the sink.
type TopicDescriptor interface {
	// GetName returns topic name.
	GetName() string
	// GetID returns topic identifier.
	GetID() descpb.ID
	// GetVersion returns topic version.
	// For example, the underlying data source (e.g. table) may change, in which case
	// we may want to emit same Name/ID, but a different version number.
	GetVersion() descpb.DescriptorVersion
}

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// EmitRow enqueues a row message for asynchronous delivery on the sink. An
	// error may be returned if a previously enqueued message has failed.
	EmitRow(ctx context.Context, topic TopicDescriptor, key, value []byte, updated hlc.Timestamp) error
	// EmitResolvedTimestamp enqueues a resolved timestamp message for
	// asynchronous delivery on every topic that has been seen by EmitRow. An
	// error may be returned if a previously enqueued message has failed.
	EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error
	// Flush blocks until every message enqueued by EmitRow and
	// EmitResolvedTimestamp has been acknowledged by the sink. If an error is
	// returned, no guarantees are given about which messages have been
	// delivered or not delivered.
	Flush(ctx context.Context) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
}

func getSink(
	ctx context.Context,
	sinkURI string,
	srcID base.SQLInstanceID,
	opts map[string]string,
	targets jobspb.ChangefeedTargets,
	settings *cluster.Settings,
	timestampOracle timestampLowerBoundOracle,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	user security.SQLUsername,
	acc mon.BoundAccount,
) (Sink, error) {
	u, err := url.Parse(sinkURI)
	if err != nil {
		return nil, err
	}
	q := u.Query()

	// Use a function here to delay creation of the sink until after we've done
	// all the parameter verification.
	var makeSink func() (Sink, error)
	switch {
	case u.Scheme == changefeedbase.SinkSchemeBuffer:
		makeSink = func() (Sink, error) { return &bufferSink{}, nil }
	case u.Scheme == changefeedbase.SinkSchemeKafka:
		var cfg kafkaSinkConfig
		cfg.kafkaTopicPrefix = q.Get(changefeedbase.SinkParamTopicPrefix)
		q.Del(changefeedbase.SinkParamTopicPrefix)
		if schemaTopic := q.Get(changefeedbase.SinkParamSchemaTopic); schemaTopic != `` {
			return nil, errors.Errorf(`%s is not yet supported`, changefeedbase.SinkParamSchemaTopic)
		}
		q.Del(changefeedbase.SinkParamSchemaTopic)
		if tlsBool := q.Get(changefeedbase.SinkParamTLSEnabled); tlsBool != `` {
			var err error
			if cfg.tlsEnabled, err = strconv.ParseBool(tlsBool); err != nil {
				return nil, errors.Errorf(`param %s must be a bool: %s`, changefeedbase.SinkParamTLSEnabled, err)
			}
		}
		q.Del(changefeedbase.SinkParamTLSEnabled)
		if tlsVerifyBool := q.Get(changefeedbase.SinkParamSkipTLSVerify); tlsVerifyBool != `` {
			var err error
			if cfg.tlsSkipVerify, err = strconv.ParseBool(tlsVerifyBool); err != nil {
				return nil, errors.Errorf(`param %s must be a bool: %s`, changefeedbase.SinkParamSkipTLSVerify, err)
			}
		}
		q.Del(changefeedbase.SinkParamSkipTLSVerify)
		if caCertHex := q.Get(changefeedbase.SinkParamCACert); caCertHex != `` {
			// TODO(dan): There's a straightforward and unambiguous transformation
			// between the base 64 encoding defined in RFC 4648 and the URL variant
			// defined in the same RFC: simply replace all `+` with `-` and `/` with
			// `_`. Consider always doing this for the user and accepting either
			// variant.
			if cfg.caCert, err = base64.StdEncoding.DecodeString(caCertHex); err != nil {
				return nil, errors.Errorf(`param %s must be base 64 encoded: %s`, changefeedbase.SinkParamCACert, err)
			}
		}
		q.Del(changefeedbase.SinkParamCACert)
		if clientCertHex := q.Get(changefeedbase.SinkParamClientCert); clientCertHex != `` {
			if cfg.clientCert, err = base64.StdEncoding.DecodeString(clientCertHex); err != nil {
				return nil, errors.Errorf(`param %s must be base 64 encoded: %s`, changefeedbase.SinkParamClientCert, err)
			}
		}
		q.Del(changefeedbase.SinkParamClientCert)
		if clientKeyHex := q.Get(changefeedbase.SinkParamClientKey); clientKeyHex != `` {
			if cfg.clientKey, err = base64.StdEncoding.DecodeString(clientKeyHex); err != nil {
				return nil, errors.Errorf(`param %s must be base 64 encoded: %s`, changefeedbase.SinkParamClientKey, err)
			}
		}
		q.Del(changefeedbase.SinkParamClientKey)

		saslParam := q.Get(changefeedbase.SinkParamSASLEnabled)
		q.Del(changefeedbase.SinkParamSASLEnabled)
		if saslParam != `` {
			b, err := strconv.ParseBool(saslParam)
			if err != nil {
				return nil, errors.Wrapf(err, `param %s must be a bool:`, changefeedbase.SinkParamSASLEnabled)
			}
			cfg.saslEnabled = b
		}
		handshakeParam := q.Get(changefeedbase.SinkParamSASLHandshake)
		q.Del(changefeedbase.SinkParamSASLHandshake)
		if handshakeParam == `` {
			cfg.saslHandshake = true
		} else {
			if !cfg.saslEnabled {
				return nil, errors.Errorf(`%s must be enabled to configure SASL handshake behavior`, changefeedbase.SinkParamSASLEnabled)
			}
			b, err := strconv.ParseBool(handshakeParam)
			if err != nil {
				return nil, errors.Wrapf(err, `param %s must be a bool:`, changefeedbase.SinkParamSASLHandshake)
			}
			cfg.saslHandshake = b
		}

		cfg.saslMechanism = q.Get(changefeedbase.SinkParamSASLMechanism)
		q.Del(changefeedbase.SinkParamSASLMechanism)
		if cfg.saslMechanism != `` && !cfg.saslEnabled {
			return nil, errors.Errorf(`%s must be enabled to configure SASL mechanism`, changefeedbase.SinkParamSASLEnabled)
		}
		if cfg.saslMechanism == `` {
			cfg.saslMechanism = sarama.SASLTypePlaintext
		}
		switch cfg.saslMechanism {
		case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext:
		default:
			return nil, errors.Errorf(`param %s must be one of %s, %s, or %s`,
				changefeedbase.SinkParamSASLMechanism,
				sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext)
		}

		cfg.saslUser = q.Get(changefeedbase.SinkParamSASLUser)
		q.Del(changefeedbase.SinkParamSASLUser)
		cfg.saslPassword = q.Get(changefeedbase.SinkParamSASLPassword)
		q.Del(changefeedbase.SinkParamSASLPassword)
		if cfg.saslEnabled {
			if cfg.saslUser == `` {
				return nil, errors.Errorf(`%s must be provided when SASL is enabled`, changefeedbase.SinkParamSASLUser)
			}
			if cfg.saslPassword == `` {
				return nil, errors.Errorf(`%s must be provided when SASL is enabled`, changefeedbase.SinkParamSASLPassword)
			}
		} else {
			if cfg.saslUser != `` {
				return nil, errors.Errorf(`%s must be enabled if a SASL user is provided`, changefeedbase.SinkParamSASLEnabled)
			}
			if cfg.saslPassword != `` {
				return nil, errors.Errorf(`%s must be enabled if a SASL password is provided`, changefeedbase.SinkParamSASLEnabled)
			}
		}

		makeSink = func() (Sink, error) {
			return makeKafkaSink(ctx, cfg, u.Host, targets, opts, acc)
		}
	case isCloudStorageSink(u):
		fileSizeParam := q.Get(changefeedbase.SinkParamFileSize)
		q.Del(changefeedbase.SinkParamFileSize)
		var fileSize int64 = 16 << 20 // 16MB
		if fileSizeParam != `` {
			if fileSize, err = humanizeutil.ParseBytes(fileSizeParam); err != nil {
				return nil, pgerror.Wrapf(err, pgcode.Syntax, `parsing %s`, fileSizeParam)
			}
		}
		u.Scheme = strings.TrimPrefix(u.Scheme, `experimental-`)
		// Transfer "ownership" of validating all remaining query parameters to
		// ExternalStorage.
		u.RawQuery = q.Encode()
		q = url.Values{}
		makeSink = func() (Sink, error) {
			return makeCloudStorageSink(
				ctx, u.String(), srcID, fileSize, settings,
				opts, timestampOracle, makeExternalStorageFromURI, user, acc,
			)
		}
	case u.Scheme == changefeedbase.SinkSchemeExperimentalSQL:
		// Swap the changefeed prefix for the sql connection one that sqlSink
		// expects.
		u.Scheme = `postgres`
		// TODO(dan): Make tableName configurable or based on the job ID or
		// something.
		tableName := `sqlsink`
		makeSink = func() (Sink, error) {
			return makeSQLSink(u.String(), tableName, targets)
		}
		// Remove parameters we know about for the unknown parameter check.
		q.Del(`sslcert`)
		q.Del(`sslkey`)
		q.Del(`sslmode`)
		q.Del(`sslrootcert`)
	default:
		return nil, errors.Errorf(`unsupported sink: %s`, u.Scheme)
	}

	for k := range q {
		return nil, errors.Errorf(`unknown sink query parameter: %s`, k)
	}

	s, err := makeSink()
	if err != nil {
		return nil, err
	}
	return s, nil
}

// errorWrapperSink delegates to another sink and marks all returned errors as
// retryable. During changefeed setup, we use the sink once without this to
// verify configuration, but in the steady state, no sink error should be
// terminal.
type errorWrapperSink struct {
	wrapped Sink
}

func (s errorWrapperSink) EmitRow(
	ctx context.Context, topicDescr TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	if err := s.wrapped.EmitRow(ctx, topicDescr, key, value, updated); err != nil {
		return MarkRetryableError(err)
	}
	return nil
}

func (s errorWrapperSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if err := s.wrapped.EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return MarkRetryableError(err)
	}
	return nil
}

func (s errorWrapperSink) Flush(ctx context.Context) error {
	if err := s.wrapped.Flush(ctx); err != nil {
		return MarkRetryableError(err)
	}
	return nil
}

func (s errorWrapperSink) Close() error {
	if err := s.wrapped.Close(); err != nil {
		return MarkRetryableError(err)
	}
	return nil
}

type kafkaLogAdapter struct {
	ctx context.Context
}

var _ sarama.StdLogger = (*kafkaLogAdapter)(nil)

func (l *kafkaLogAdapter) Print(v ...interface{}) {
	log.InfofDepth(l.ctx, 1, "", v...)
}
func (l *kafkaLogAdapter) Printf(format string, v ...interface{}) {
	log.InfofDepth(l.ctx, 1, format, v...)
}
func (l *kafkaLogAdapter) Println(v ...interface{}) {
	log.InfofDepth(l.ctx, 1, "", v...)
}

func init() {
	// We'd much prefer to make one of these per sink, so we can use the real
	// context, but quite unfortunately, sarama only has a global logger hook.
	ctx := context.Background()
	ctx = logtags.AddTag(ctx, "kafka-producer", nil)
	sarama.Logger = &kafkaLogAdapter{ctx: ctx}
}

type kafkaSinkConfig struct {
	kafkaTopicPrefix string
	tlsEnabled       bool
	tlsSkipVerify    bool
	caCert           []byte
	clientCert       []byte
	clientKey        []byte
	saslEnabled      bool
	saslHandshake    bool
	saslUser         string
	saslPassword     string
	saslMechanism    string
	targetNames      map[descpb.ID]string
}

// kafkaSink emits to Kafka asynchronously. It is not concurrency-safe; all
// calls to Emit and Flush should be from the same goroutine.
type kafkaSink struct {
	ctx      context.Context
	cfg      kafkaSinkConfig
	client   sarama.Client
	producer sarama.AsyncProducer
	topics   map[string]struct{}

	lastMetadataRefresh time.Time

	stopWorkerCh chan struct{}
	worker       sync.WaitGroup
	scratch      bufalloc.ByteAllocator

	// Only synchronized between the client goroutine and the worker goroutine.
	mu struct {
		syncutil.Mutex
		mem      mon.BoundAccount
		inflight int64
		flushErr error
		flushCh  chan struct{}
	}
}

func (s *kafkaSink) setTargets(targets jobspb.ChangefeedTargets) {
	s.topics = make(map[string]struct{})
	s.cfg.targetNames = make(map[descpb.ID]string)
	for id, t := range targets {
		s.cfg.targetNames[id] = t.StatementTimeName
		s.topics[s.cfg.kafkaTopicPrefix+SQLNameToKafkaName(t.StatementTimeName)] = struct{}{}
	}
}

type jsonDuration time.Duration

func (j *jsonDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*j = jsonDuration(dur)
	return nil
}

type saramaConfig struct {
	// These settings mirror ones in sarama config.
	// We just tag them w/ JSON annotations.
	// Flush describes settings specific to producer flushing.
	// See sarama.Config.Producer.Flush
	Flush struct {
		Bytes       int          `json:",omitempty"`
		Messages    int          `json:",omitempty"`
		Frequency   jsonDuration `json:",omitempty"`
		MaxMessages int          `json:",omitempty"`
	}
	RequiredAcks string `json:",omitempty"`

	Version string `json:",omitempty"`
}

// Configure configures provided kafka configuration struct based
// on this config.
func (c *saramaConfig) Apply(kafka *sarama.Config) error {
	kafka.Producer.Flush.Bytes = c.Flush.Bytes
	kafka.Producer.Flush.Messages = c.Flush.Messages
	kafka.Producer.Flush.Frequency = time.Duration(c.Flush.Frequency)
	kafka.Producer.Flush.MaxMessages = c.Flush.MaxMessages
	if c.Version != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(c.Version)
		if err != nil {
			return err
		}
		kafka.Version = parsedVersion
	}
	if c.RequiredAcks != "" {
		parsedAcks, err := parseRequiredAcks(c.RequiredAcks)
		if err != nil {
			return err
		}
		kafka.Producer.RequiredAcks = parsedAcks
	}
	return nil
}

func parseRequiredAcks(a string) (sarama.RequiredAcks, error) {
	switch a {
	case "0", "NONE":
		return sarama.NoResponse, nil
	case "1", "ONE":
		return sarama.WaitForLocal, nil
	case "-1", "ALL":
		return sarama.WaitForAll, nil
	default:
		return sarama.WaitForLocal,
			fmt.Errorf(`invalid acks value "%s", must be "NONE"/"0", "ONE"/"1", or "ALL"/"-1"`, a)
	}
}

func (c saramaConfig) Validate() error {
	// If Flush.Bytes > 0 or Flush.Messages > 1 without
	// Flush.Frequency, sarama may wait forever to flush the
	// messages to Kafka.  We want to guard against such
	// configurations to ensure that we don't get into a situation
	// where our call to Flush() would block forever.
	if (c.Flush.Bytes > 0 || c.Flush.Messages > 1) && c.Flush.Frequency == 0 {
		return errors.New("Flush.Frequency must be > 0 when Flush.Bytes > 0 or Flush.Messages > 1")
	}
	return nil
}

func defaultSaramaConfig() *saramaConfig {
	config := &saramaConfig{}

	// When we emit messages to sarama, they're placed in a queue
	// (as does any reasonable kafka producer client). When our
	// sink's Flush is called, we have to wait for all buffered
	// and inflight requests to be sent and then
	// acknowledged. Quite unfortunately, we have no way to hint
	// to the producer that it should immediately send out
	// whatever is buffered. This configuration can have a
	// dramatic impact on how quickly this happens naturally (and
	// some configurations will block forever!).
	//
	// The default configuration of all 0 values will send
	// messages as quickly as possible.
	config.Flush.Messages = 0
	config.Flush.Frequency = jsonDuration(0)
	config.Flush.Bytes = 0

	// This works around what seems to be a bug in sarama where it isn't
	// computing the right value to compare against `Producer.MaxMessageBytes`
	// and the server sends it back with a "Message was too large, server
	// rejected it to avoid allocation" error. The other flush tunings are
	// hints, but this one is a hard limit, so it's useful here as a workaround.
	//
	// This workaround should probably be something like setting
	// `Producer.MaxMessageBytes` to 90% of it's value for some headroom, but
	// this workaround is the one that's been running in roachtests and I'd want
	// to test this one more before changing it.
	config.Flush.MaxMessages = 1000

	return config
}

func getSaramaConfig(opts map[string]string) (config *saramaConfig, err error) {
	config = defaultSaramaConfig()
	if configStr, haveOverride := opts[changefeedbase.OptKafkaSinkConfig]; haveOverride {
		config = &saramaConfig{}
		err = json.Unmarshal([]byte(configStr), config)
	}
	return
}

func makeKafkaSink(
	ctx context.Context,
	cfg kafkaSinkConfig,
	bootstrapServers string,
	targets jobspb.ChangefeedTargets,
	opts map[string]string,
	acc mon.BoundAccount,
) (Sink, error) {
	sink := &kafkaSink{
		ctx: ctx,
		cfg: cfg,
	}
	sink.setTargets(targets)
	sink.mu.mem = acc

	config := sarama.NewConfig()
	config.ClientID = `CockroachDB`
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = newChangefeedPartitioner

	if cfg.caCert != nil {
		if !cfg.tlsEnabled {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamCACert, changefeedbase.SinkParamTLSEnabled)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(cfg.caCert)
		config.Net.TLS.Config = &tls.Config{
			RootCAs: caCertPool,
		}
		config.Net.TLS.Enable = true
	} else if cfg.tlsEnabled {
		config.Net.TLS.Enable = true
	}

	if cfg.tlsEnabled {
		if config.Net.TLS.Config == nil {
			config.Net.TLS.Config = &tls.Config{}
		}
		config.Net.TLS.Config.InsecureSkipVerify = cfg.tlsSkipVerify
	}

	if cfg.clientCert != nil {
		if !cfg.tlsEnabled {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamTLSEnabled)
		}
		if cfg.clientKey == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamClientKey)
		}
		cert, err := tls.X509KeyPair(cfg.clientCert, cfg.clientKey)
		if err != nil {
			return nil, errors.Errorf(`invalid client certificate data provided: %s`, err)
		}
		config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
	} else if cfg.clientKey != nil {
		return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientKey, changefeedbase.SinkParamClientCert)
	}

	if cfg.saslEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = cfg.saslHandshake
		config.Net.SASL.User = cfg.saslUser
		config.Net.SASL.Password = cfg.saslPassword
		config.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.saslMechanism)
		switch config.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA512:
			config.Net.SASL.SCRAMClientGeneratorFunc = sha512ClientGenerator
		case sarama.SASLTypeSCRAMSHA256:
			config.Net.SASL.SCRAMClientGeneratorFunc = sha256ClientGenerator
		}
	}

	saramaCfg, err := getSaramaConfig(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse sarama config; check changefeed.experimental_kafka_config setting")
	}

	if err := saramaCfg.Apply(config); err != nil {
		return nil, errors.Wrap(err, "failed to apply kafka client configuration")
	}

	if err := saramaCfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid sarama configuration")
	}

	sink.client, err = sarama.NewClient(strings.Split(bootstrapServers, `,`), config)
	if err != nil {
		err = pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, bootstrapServers)
		return nil, err
	}
	sink.producer, err = sarama.NewAsyncProducerFromClient(sink.client)
	if err != nil {
		err = pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, bootstrapServers)
		return nil, err
	}

	sink.start()
	return sink, nil
}

func (s *kafkaSink) start() {
	s.stopWorkerCh = make(chan struct{})
	s.worker.Add(1)
	go s.workerLoop()
}

// Close implements the Sink interface.
func (s *kafkaSink) Close() error {
	defer func() {
		s.mu.Lock()
		s.mu.mem.Close(s.ctx)
		s.mu.Unlock()
	}()

	close(s.stopWorkerCh)
	s.worker.Wait()
	// If we're shutting down, we don't care what happens to the outstanding
	// messages, so ignore this error.
	_ = s.producer.Close()
	// s.client is only nil in tests.
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// EmitRow implements the Sink interface.
func (s *kafkaSink) EmitRow(
	ctx context.Context, topicDescr TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	topic := s.cfg.kafkaTopicPrefix + SQLNameToKafkaName(s.cfg.targetNames[topicDescr.GetID()])
	if _, ok := s.topics[topic]; !ok {
		return errors.Errorf(`cannot emit to undeclared topic: %s`, topic)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	return s.emitMessage(ctx, msg)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *kafkaSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	// Periodically ping sarama to refresh its metadata. This means talking to
	// zookeeper, so it shouldn't be done too often, but beyond that this
	// constant was picked pretty arbitrarily.
	//
	// TODO(dan): Add a test for this. We can't right now (2018-11-13) because
	// we'd need to bump sarama, but that's a bad idea while we're still
	// actively working on stability. At the same time, revisit this tuning.
	const metadataRefreshMinDuration = time.Minute
	if timeutil.Since(s.lastMetadataRefresh) > metadataRefreshMinDuration {
		topics := make([]string, 0, len(s.topics))
		for topic := range s.topics {
			topics = append(topics, topic)
		}
		if err := s.client.RefreshMetadata(topics...); err != nil {
			return err
		}
		s.lastMetadataRefresh = timeutil.Now()
	}

	for topic := range s.topics {
		payload, err := encoder.EncodeResolvedTimestamp(ctx, topic, resolved)
		if err != nil {
			return err
		}
		s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)

		// sarama caches this, which is why we have to periodically refresh the
		// metadata above. Staleness here does not impact correctness. Some new
		// partitions will miss this resolved timestamp, but they'll eventually
		// be picked up and get later ones.
		partitions, err := s.client.Partitions(topic)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			msg := &sarama.ProducerMessage{
				Topic:     topic,
				Partition: partition,
				Key:       nil,
				Value:     sarama.ByteEncoder(payload),
			}
			if err := s.emitMessage(ctx, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

// Flush implements the Sink interface.
func (s *kafkaSink) Flush(ctx context.Context) error {
	flushCh := make(chan struct{}, 1)

	s.mu.Lock()
	inflight := s.mu.inflight
	flushErr := s.mu.flushErr
	s.mu.flushErr = nil
	immediateFlush := inflight == 0 || flushErr != nil
	if !immediateFlush {
		s.mu.flushCh = flushCh
	}
	s.mu.Unlock()

	if immediateFlush {
		return flushErr
	}

	if log.V(1) {
		log.Infof(ctx, "flush waiting for %d inflight messages", inflight)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flushCh:
		s.mu.Lock()
		flushErr := s.mu.flushErr
		s.mu.flushErr = nil
		s.mu.Unlock()
		return flushErr
	}
}

func kafkaMessageBytes(m *sarama.ProducerMessage) (s int64) {
	if m.Key != nil {
		s += int64(m.Key.Length())
	}
	if m.Value != nil {
		s += int64(m.Value.Length())
	}
	return
}

func (s *kafkaSink) startInflightMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mu.mem.Grow(ctx, kafkaMessageBytes(msg)); err != nil {
		return err
	}

	s.mu.inflight++

	if log.V(2) {
		log.Infof(ctx, "emitting %d inflight records to kafka", s.mu.inflight)
	}
	return nil
}

func (s *kafkaSink) emitMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	if err := s.startInflightMessage(ctx, msg); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.producer.Input() <- msg:
	}

	return nil
}

func (s *kafkaSink) workerLoop() {
	defer s.worker.Done()

	for {
		var ackMsg *sarama.ProducerMessage
		var ackError error

		select {
		case <-s.stopWorkerCh:
			return
		case m := <-s.producer.Successes():
			ackMsg = m
		case err := <-s.producer.Errors():
			ackMsg, ackError = err.Msg, err.Err
		}

		s.mu.Lock()
		s.mu.inflight--
		s.mu.mem.Shrink(s.ctx, kafkaMessageBytes(ackMsg))
		if s.mu.flushErr == nil && ackError != nil {
			s.mu.flushErr = ackError
		}

		if s.mu.inflight == 0 && s.mu.flushCh != nil {
			s.mu.flushCh <- struct{}{}
			s.mu.flushCh = nil
		}
		s.mu.Unlock()
	}
}

type changefeedPartitioner struct {
	hash sarama.Partitioner
}

var _ sarama.Partitioner = &changefeedPartitioner{}
var _ sarama.PartitionerConstructor = newChangefeedPartitioner

func newChangefeedPartitioner(topic string) sarama.Partitioner {
	return &changefeedPartitioner{
		hash: sarama.NewHashPartitioner(topic),
	}
}

func (p *changefeedPartitioner) RequiresConsistency() bool { return true }
func (p *changefeedPartitioner) Partition(
	message *sarama.ProducerMessage, numPartitions int32,
) (int32, error) {
	if message.Key == nil {
		return message.Partition, nil
	}
	return p.hash.Partition(message, numPartitions)
}

const (
	sqlSinkCreateTableStmt = `CREATE TABLE IF NOT EXISTS "%s" (
		topic STRING,
		partition INT,
		message_id INT,
		key BYTES, value BYTES,
		resolved BYTES,
		PRIMARY KEY (topic, partition, message_id)
	)`
	sqlSinkEmitStmt = `INSERT INTO "%s" (topic, partition, message_id, key, value, resolved)`
	sqlSinkEmitCols = 6
	// Some amount of batching to mirror a bit how kafkaSink works.
	sqlSinkRowBatchSize = 3
	// While sqlSink is only used for testing, hardcode the number of
	// partitions to something small but greater than 1.
	sqlSinkNumPartitions = 3
)

// sqlSink mirrors the semantics offered by kafkaSink as closely as possible,
// but writes to a SQL table (presumably in CockroachDB). Currently only for
// testing.
//
// Each emitted row or resolved timestamp is stored as a row in the table. Each
// table gets 3 partitions. Similar to kafkaSink, the order between two emits is
// only preserved if they are emitted to by the same node and to the same
// partition.
type sqlSink struct {
	db *gosql.DB

	tableName string
	topics    map[string]struct{}
	hasher    hash.Hash32

	rowBuf  []interface{}
	scratch bufalloc.ByteAllocator

	targetNames map[descpb.ID]string
}

func makeSQLSink(uri, tableName string, targets jobspb.ChangefeedTargets) (*sqlSink, error) {
	if u, err := url.Parse(uri); err != nil {
		return nil, err
	} else if u.Path == `` {
		return nil, errors.Errorf(`must specify database`)
	}
	db, err := gosql.Open(`postgres`, uri)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(fmt.Sprintf(sqlSinkCreateTableStmt, tableName)); err != nil {
		db.Close()
		return nil, err
	}

	s := &sqlSink{
		db:          db,
		tableName:   tableName,
		topics:      make(map[string]struct{}),
		hasher:      fnv.New32a(),
		targetNames: make(map[descpb.ID]string),
	}
	for id, t := range targets {
		s.topics[t.StatementTimeName] = struct{}{}
		s.targetNames[id] = t.StatementTimeName
	}
	return s, nil
}

// EmitRow implements the Sink interface.
func (s *sqlSink) EmitRow(
	ctx context.Context, topicDescr TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	topic := s.targetNames[topicDescr.GetID()]
	if _, ok := s.topics[topic]; !ok {
		return errors.Errorf(`cannot emit to undeclared topic: %s`, topic)
	}

	// Hashing logic copied from sarama.HashPartitioner.
	s.hasher.Reset()
	if _, err := s.hasher.Write(key); err != nil {
		return err
	}
	partition := int32(s.hasher.Sum32()) % sqlSinkNumPartitions
	if partition < 0 {
		partition = -partition
	}

	var noResolved []byte
	return s.emit(ctx, topic, partition, key, value, noResolved)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *sqlSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	var noKey, noValue []byte
	for topic := range s.topics {
		payload, err := encoder.EncodeResolvedTimestamp(ctx, topic, resolved)
		if err != nil {
			return err
		}
		s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)
		for partition := int32(0); partition < sqlSinkNumPartitions; partition++ {
			if err := s.emit(ctx, topic, partition, noKey, noValue, payload); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sqlSink) emit(
	ctx context.Context, topic string, partition int32, key, value, resolved []byte,
) error {
	// Generate the message id on the client to match the guaranttees of kafka
	// (two messages are only guaranteed to keep their order if emitted from the
	// same producer to the same partition).
	messageID := builtins.GenerateUniqueInt(base.SQLInstanceID(partition))
	s.rowBuf = append(s.rowBuf, topic, partition, messageID, key, value, resolved)
	if len(s.rowBuf)/sqlSinkEmitCols >= sqlSinkRowBatchSize {
		return s.Flush(ctx)
	}
	return nil
}

// Flush implements the Sink interface.
func (s *sqlSink) Flush(ctx context.Context) error {
	if len(s.rowBuf) == 0 {
		return nil
	}

	var stmt strings.Builder
	fmt.Fprintf(&stmt, sqlSinkEmitStmt, s.tableName)
	for i := 0; i < len(s.rowBuf); i++ {
		if i == 0 {
			stmt.WriteString(` VALUES (`)
		} else if i%sqlSinkEmitCols == 0 {
			stmt.WriteString(`),(`)
		} else {
			stmt.WriteString(`,`)
		}
		fmt.Fprintf(&stmt, `$%d`, i+1)
	}
	stmt.WriteString(`)`)
	_, err := s.db.Exec(stmt.String(), s.rowBuf...)
	if err != nil {
		return err
	}
	s.rowBuf = s.rowBuf[:0]
	return nil
}

// Close implements the Sink interface.
func (s *sqlSink) Close() error {
	return s.db.Close()
}

// encDatumRowBuffer is a FIFO of `EncDatumRow`s.
//
// TODO(dan): There's some potential allocation savings here by reusing the same
// backing array.
type encDatumRowBuffer []rowenc.EncDatumRow

func (b *encDatumRowBuffer) IsEmpty() bool {
	return b == nil || len(*b) == 0
}
func (b *encDatumRowBuffer) Push(r rowenc.EncDatumRow) {
	*b = append(*b, r)
}
func (b *encDatumRowBuffer) Pop() rowenc.EncDatumRow {
	ret := (*b)[0]
	*b = (*b)[1:]
	return ret
}

type bufferSink struct {
	buf     encDatumRowBuffer
	alloc   rowenc.DatumAlloc
	scratch bufalloc.ByteAllocator
	closed  bool
}

// EmitRow implements the Sink interface.
func (s *bufferSink) EmitRow(
	ctx context.Context, topic TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	if s.closed {
		return errors.New(`cannot EmitRow on a closed sink`)
	}
	s.buf.Push(rowenc.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: s.alloc.NewDString(tree.DString(topic.GetName()))}, // topic
		{Datum: s.alloc.NewDBytes(tree.DBytes(key))},               // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(value))},             // value
	})
	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *bufferSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if s.closed {
		return errors.New(`cannot EmitResolvedTimestamp on a closed sink`)
	}
	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(ctx, noTopic, resolved)
	if err != nil {
		return err
	}
	s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)
	s.buf.Push(rowenc.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: tree.DNull}, // topic
		{Datum: tree.DNull}, // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(payload))}, // value
	})
	return nil
}

// Flush implements the Sink interface.
func (s *bufferSink) Flush(_ context.Context) error {
	return nil
}

// Close implements the Sink interface.
func (s *bufferSink) Close() error {
	s.closed = true
	return nil
}

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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// EmitRow enqueues a row message for asynchronous delivery on the sink. An
	// error may be returned if a previously enqueued message has failed.
	EmitRow(
		ctx context.Context,
		table *sqlbase.TableDescriptor,
		key, value []byte,
		updated hlc.Timestamp,
	) error
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
	nodeID roachpb.NodeID,
	opts map[string]string,
	targets jobspb.ChangefeedTargets,
	settings *cluster.Settings,
	timestampOracle timestampLowerBoundOracle,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	user string,
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
			return makeKafkaSink(cfg, u.Host, targets)
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
				ctx, u.String(), nodeID, fileSize, settings,
				opts, timestampOracle, makeExternalStorageFromURI, user,
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
	ctx context.Context, table *sqlbase.TableDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	if err := s.wrapped.EmitRow(ctx, table, key, value, updated); err != nil {
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
	caCert           []byte
	clientCert       []byte
	clientKey        []byte
	saslEnabled      bool
	saslHandshake    bool
	saslUser         string
	saslPassword     string
}

// kafkaSink emits to Kafka asynchronously. It is not concurrency-safe; all
// calls to Emit and Flush should be from the same goroutine.
type kafkaSink struct {
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
		inflight int64
		flushErr error
		flushCh  chan struct{}
	}
}

func makeKafkaSink(
	cfg kafkaSinkConfig, bootstrapServers string, targets jobspb.ChangefeedTargets,
) (Sink, error) {
	sink := &kafkaSink{cfg: cfg}
	sink.topics = make(map[string]struct{})
	for _, t := range targets {
		sink.topics[cfg.kafkaTopicPrefix+SQLNameToKafkaName(t.StatementTimeName)] = struct{}{}
	}

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
		if config.Net.TLS.Config == nil {
			config.Net.TLS.Config = &tls.Config{}
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
	}

	// When we emit messages to sarama, they're placed in a queue (as does any
	// reasonable kafka producer client). When our sink's Flush is called, we
	// have to wait for all buffered and inflight requests to be sent and then
	// acknowledged. Quite unfortunately, we have no way to hint to the producer
	// that it should immediately send out whatever is buffered. This
	// configuration can have a dramatic impact on how quickly this happens
	// naturally (and some configurations will block forever!).
	//
	// We can configure the producer to send out its batches based on number of
	// messages and/or total buffered message size and/or time. If none of them
	// are set, it uses some defaults, but if any of the three are set, it does
	// no defaulting. Which means that if `Flush.Messages` is set to 10 and
	// nothing else is set, then 9/10 times `Flush` will block forever. We can
	// work around this by also setting `Flush.Frequency` but a cleaner way is
	// to set `Flush.Messages` to 1. In the steady state, this sends a request
	// with some messages, buffers any messages that come in while it is in
	// flight, then sends those out.
	config.Producer.Flush.Messages = 1

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
	config.Producer.Flush.MaxMessages = 1000

	// config.Producer.Flush.Messages is set to 1 so we don't need this, but
	// sarama prints scary things to the logs if we don't.
	config.Producer.Flush.Frequency = time.Hour

	var err error
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
	ctx context.Context, table *sqlbase.TableDescriptor, key, value []byte, _ hlc.Timestamp,
) error {
	topic := s.cfg.kafkaTopicPrefix + SQLNameToKafkaName(table.Name)
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

func (s *kafkaSink) emitMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	s.mu.Lock()
	s.mu.inflight++
	inflight := s.mu.inflight
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.producer.Input() <- msg:
	}

	if log.V(2) {
		log.Infof(ctx, "emitted %d inflight records to kafka", inflight)
	}
	return nil
}

func (s *kafkaSink) workerLoop() {
	defer s.worker.Done()

	for {
		select {
		case <-s.stopWorkerCh:
			return
		case <-s.producer.Successes():
		case err := <-s.producer.Errors():
			s.mu.Lock()
			if s.mu.flushErr == nil {
				s.mu.flushErr = err
			}
			s.mu.Unlock()
		}

		s.mu.Lock()
		s.mu.inflight--
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
		db:        db,
		tableName: tableName,
		topics:    make(map[string]struct{}),
		hasher:    fnv.New32a(),
	}
	for _, t := range targets {
		s.topics[t.StatementTimeName] = struct{}{}
	}
	return s, nil
}

// EmitRow implements the Sink interface.
func (s *sqlSink) EmitRow(
	ctx context.Context, table *sqlbase.TableDescriptor, key, value []byte, _ hlc.Timestamp,
) error {
	topic := table.Name
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
type encDatumRowBuffer []sqlbase.EncDatumRow

func (b *encDatumRowBuffer) IsEmpty() bool {
	return b == nil || len(*b) == 0
}
func (b *encDatumRowBuffer) Push(r sqlbase.EncDatumRow) {
	*b = append(*b, r)
}
func (b *encDatumRowBuffer) Pop() sqlbase.EncDatumRow {
	ret := (*b)[0]
	*b = (*b)[1:]
	return ret
}

type bufferSink struct {
	buf     encDatumRowBuffer
	alloc   sqlbase.DatumAlloc
	scratch bufalloc.ByteAllocator
	closed  bool
}

// EmitRow implements the Sink interface.
func (s *bufferSink) EmitRow(
	_ context.Context, table *sqlbase.TableDescriptor, key, value []byte, _ hlc.Timestamp,
) error {
	if s.closed {
		return errors.New(`cannot EmitRow on a closed sink`)
	}
	topic := table.Name
	s.buf.Push(sqlbase.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: s.alloc.NewDString(tree.DString(topic))}, // topic
		{Datum: s.alloc.NewDBytes(tree.DBytes(key))},     // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(value))},   //value
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
	s.buf.Push(sqlbase.EncDatumRow{
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

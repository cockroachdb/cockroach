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
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

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

// kafkaClient is a small interface restricting the functionality in sarama.Client
type kafkaClient interface {
	// Partitions returns the sorted list of all partition IDs for the given topic.
	Partitions(topic string) ([]int32, error)
	// RefreshMetadata takes a list of topics and queries the cluster to refresh the
	// available metadata for those topics. If no topics are provided, it will refresh
	// metadata for all topics.
	RefreshMetadata(topics ...string) error
	// Close closes kafka connection.
	Close() error
}

// kafkaSink emits to Kafka asynchronously. It is not concurrency-safe; all
// calls to Emit and Flush should be from the same goroutine.
type kafkaSink struct {
	ctx            context.Context
	bootstrapAddrs string
	kafkaCfg       *sarama.Config
	client         kafkaClient
	producer       sarama.AsyncProducer
	topics         map[descpb.ID]string

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
}

var defaultSaramaConfig = func() *saramaConfig {
	config := &saramaConfig{}

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
	config.Flush.Messages = 1

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

	// config.Producer.Flush.Messages is set to 1 so we don't need this, but
	// sarama prints scary things to the logs if we don't.
	config.Flush.Frequency = jsonDuration(time.Hour)

	return config
}()

func (s *kafkaSink) start() {
	s.stopWorkerCh = make(chan struct{})
	s.worker.Add(1)
	go s.workerLoop()
}

// Dial implements the Sink interface.
func (s *kafkaSink) Dial() error {
	client, err := sarama.NewClient(strings.Split(s.bootstrapAddrs, `,`), s.kafkaCfg)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, s.bootstrapAddrs)
	}
	s.producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, s.bootstrapAddrs)
	}
	s.client = client
	s.start()
	return nil
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
	topic, isKnownTopic := s.topics[topicDescr.GetID()]
	if !isKnownTopic {
		return errors.Errorf(`cannot emit to undeclared topic: %s`, topicDescr.GetName())
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
		for _, topic := range s.topics {
			topics = append(topics, topic)
		}
		if err := s.client.RefreshMetadata(topics...); err != nil {
			return err
		}
		s.lastMetadataRefresh = timeutil.Now()
	}

	for _, topic := range s.topics {
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

func makeTopicsMap(
	prefix string, name string, targets jobspb.ChangefeedTargets,
) map[descpb.ID]string {
	topics := make(map[descpb.ID]string)
	useSingleName := name != ""
	if useSingleName {
		name = prefix + SQLNameToKafkaName(name)
	}
	for id, t := range targets {
		if useSingleName {
			topics[id] = name
		} else {
			topics[id] = prefix + SQLNameToKafkaName(t.StatementTimeName)
		}
	}
	return topics
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

// Apply configures provided kafka configuration struct based on this config.
func (c *saramaConfig) Apply(kafka *sarama.Config) {
	kafka.Producer.Flush.Bytes = c.Flush.Bytes
	kafka.Producer.Flush.Messages = c.Flush.Messages
	kafka.Producer.Flush.Frequency = time.Duration(c.Flush.Frequency)
	kafka.Producer.Flush.MaxMessages = c.Flush.MaxMessages
}

func getSaramaConfig(opts map[string]string) (config *saramaConfig, err error) {
	if configStr, haveOverride := opts[changefeedbase.OptKafkaSinkConfig]; haveOverride {
		config = &saramaConfig{}
		err = json.Unmarshal([]byte(configStr), config)
	} else {
		config = defaultSaramaConfig
	}
	return
}

func buildKafkaConfig(u sinkURL, opts map[string]string) (*sarama.Config, error) {
	dialConfig := struct {
		tlsEnabled    bool
		tlsSkipVerify bool
		caCert        []byte
		clientCert    []byte
		clientKey     []byte
		saslEnabled   bool
		saslHandshake bool
		saslUser      string
		saslPassword  string
		saslMechanism string
	}{}

	consumeBool := func(param string, dest *bool) (wasSet bool, err error) {
		if paramVal := u.consumeParam(param); paramVal != "" {
			b, err := strconv.ParseBool(paramVal)
			if err != nil {
				return false, errors.Wrapf(err, "param %s must be a bool", param)
			}
			*dest = b
			return true, nil
		}
		return false, nil
	}

	decodeBase64 := func(param string, dest *[]byte) error {
		// TODO(dan): There's a straightforward and unambiguous transformation
		//  between the base 64 encoding defined in RFC 4648 and the URL variant
		//  defined in the same RFC: simply replace all `+` with `-` and `/` with
		//  `_`. Consider always doing this for the user and accepting either
		//  variant.
		val := u.consumeParam(param)
		if val == `` {
			return nil
		}
		decoded, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return errors.Wrapf(err, `param %s must be base 64 encoded`, param)
		}
		*dest = decoded
		return nil
	}

	if _, err := consumeBool(changefeedbase.SinkParamTLSEnabled, &dialConfig.tlsEnabled); err != nil {
		return nil, err
	}
	if _, err := consumeBool(changefeedbase.SinkParamSkipTLSVerify, &dialConfig.tlsSkipVerify); err != nil {
		return nil, err
	}
	if err := decodeBase64(changefeedbase.SinkParamCACert, &dialConfig.caCert); err != nil {
		return nil, err
	}
	if err := decodeBase64(changefeedbase.SinkParamClientCert, &dialConfig.clientCert); err != nil {
		return nil, err
	}
	if err := decodeBase64(changefeedbase.SinkParamClientKey, &dialConfig.clientKey); err != nil {
		return nil, err
	}

	if _, err := consumeBool(changefeedbase.SinkParamSASLEnabled, &dialConfig.saslEnabled); err != nil {
		return nil, err
	}

	if wasSet, err := consumeBool(changefeedbase.SinkParamSASLHandshake, &dialConfig.saslHandshake); !wasSet && err == nil {
		dialConfig.saslHandshake = true
	} else {
		if err != nil {
			return nil, err
		}
		if !dialConfig.saslEnabled {
			return nil, errors.Errorf(`%s must be enabled to configure SASL handshake behavior`, changefeedbase.SinkParamSASLEnabled)
		}
	}

	dialConfig.saslMechanism = u.consumeParam(changefeedbase.SinkParamSASLMechanism)
	if dialConfig.saslMechanism != `` && !dialConfig.saslEnabled {
		return nil, errors.Errorf(`%s must be enabled to configure SASL mechanism`, changefeedbase.SinkParamSASLEnabled)
	}
	if dialConfig.saslMechanism == `` {
		dialConfig.saslMechanism = sarama.SASLTypePlaintext
	}
	switch dialConfig.saslMechanism {
	case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext:
	default:
		return nil, errors.Errorf(`param %s must be one of %s, %s, or %s`,
			changefeedbase.SinkParamSASLMechanism,
			sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext)
	}

	dialConfig.saslUser = u.consumeParam(changefeedbase.SinkParamSASLUser)
	dialConfig.saslPassword = u.consumeParam(changefeedbase.SinkParamSASLPassword)
	if dialConfig.saslEnabled {
		if dialConfig.saslUser == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, changefeedbase.SinkParamSASLUser)
		}
		if dialConfig.saslPassword == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, changefeedbase.SinkParamSASLPassword)
		}
	} else {
		if dialConfig.saslUser != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL user is provided`, changefeedbase.SinkParamSASLEnabled)
		}
		if dialConfig.saslPassword != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL password is provided`, changefeedbase.SinkParamSASLEnabled)
		}
	}

	config := sarama.NewConfig()
	config.ClientID = `CockroachDB`
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = newChangefeedPartitioner

	if dialConfig.tlsEnabled {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: dialConfig.tlsSkipVerify,
		}

		if dialConfig.caCert != nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(dialConfig.caCert)
			config.Net.TLS.Config.RootCAs = caCertPool
		}

		if dialConfig.clientCert != nil && dialConfig.clientKey == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamClientKey)
		} else if dialConfig.clientKey != nil && dialConfig.clientCert == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientKey, changefeedbase.SinkParamClientCert)
		}

		if dialConfig.clientCert != nil && dialConfig.clientKey != nil {
			cert, err := tls.X509KeyPair(dialConfig.clientCert, dialConfig.clientKey)
			if err != nil {
				return nil, errors.Errorf(`invalid client certificate data provided: %s`, err)
			}
			config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
		}
	} else {
		if dialConfig.caCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamCACert, changefeedbase.SinkParamTLSEnabled)
		}
		if dialConfig.clientCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamTLSEnabled)
		}
	}

	if dialConfig.saslEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = dialConfig.saslHandshake
		config.Net.SASL.User = dialConfig.saslUser
		config.Net.SASL.Password = dialConfig.saslPassword
		config.Net.SASL.Mechanism = sarama.SASLMechanism(dialConfig.saslMechanism)
		switch config.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA512:
			config.Net.SASL.SCRAMClientGeneratorFunc = sha512ClientGenerator
		case sarama.SASLTypeSCRAMSHA256:
			config.Net.SASL.SCRAMClientGeneratorFunc = sha256ClientGenerator
		}
	}

	// Apply statement level overrides.
	saramaCfg, err := getSaramaConfig(opts)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to parse sarama config; check %s option", changefeedbase.OptKafkaSinkConfig)
	}
	saramaCfg.Apply(config)
	return config, nil
}

func makeKafkaSink(
	ctx context.Context,
	u sinkURL,
	targets jobspb.ChangefeedTargets,
	opts map[string]string,
	acc mon.BoundAccount,
) (Sink, error) {
	kafkaTopicPrefix := u.consumeParam(changefeedbase.SinkParamTopicPrefix)
	kafkaTopicName := u.consumeParam(changefeedbase.SinkParamTopicName)
	if schemaTopic := u.consumeParam(changefeedbase.SinkParamSchemaTopic); schemaTopic != `` {
		return nil, errors.Errorf(`%s is not yet supported`, changefeedbase.SinkParamSchemaTopic)
	}

	config, err := buildKafkaConfig(u, opts)
	if err != nil {
		return nil, err
	}

	sink := &kafkaSink{
		ctx:            ctx,
		kafkaCfg:       config,
		bootstrapAddrs: u.Host,
		topics:         makeTopicsMap(kafkaTopicPrefix, kafkaTopicName, targets),
	}
	sink.mu.mem = acc

	if unknownParams := u.remainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown kafka sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return sink, nil
}

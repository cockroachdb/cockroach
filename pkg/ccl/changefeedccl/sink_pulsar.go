// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	"hash"
	"hash/crc32"
	"net/url"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// PulsarClient extracts relevant methods from pulsar.Client.
type PulsarClient interface {
	// CreateProducer implements the pulsar.Client interface.
	CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error)
	// Close implements the pulsar.Client interface.
	Close()
}

func isPulsarSink(u *url.URL) bool {
	return u.Scheme == changefeedbase.SinkSchemePulsar
}

type pulsarSink struct {
	format      changefeedbase.FormatType
	parallelism uint32
	hasher      hash.Hash32
	topicNamer  *TopicNamer
	metrics     metricsRecorder
	knobs       *TestingKnobs

	// The URL passed to the underlying client for connecting.
	// This is the changefeed's URL with the changefeed specific parameters removed.
	clientURL string

	// errCh holds an error which is set by callbacks that are executed by the downstream library.
	// The error should be checked when flushing the sink. This is implemented
	// as a channel because it must be thread safe given that callbacks may run asynchronously.
	errCh chan error

	// Initialized after Dial()ing.
	topicProducers map[string]pulsar.Producer
	client         PulsarClient
}

func (p *pulsarSink) getConcreteType() sinkType {
	return sinkTypePulsar
}

// Dial implements the Sink interface.
func (p *pulsarSink) Dial() error {
	if p.knobs != nil && p.knobs.PulsarClientSkipCreation {
		return nil
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: p.clientURL,
		// TODO(#118898): configure timeouts, memory limits, and auth options.
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		return err
	}
	p.client = client
	return p.initTopicProducers()
}

func (p *pulsarSink) initTopicProducers() error {
	// Initialize a producer for each topic.
	err := p.topicNamer.Each(func(topic string) error {
		producer, err := p.client.CreateProducer(pulsar.ProducerOptions{
			Topic:              topic,
			BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
		})
		if err != nil {
			return err
		}
		p.topicProducers[topic] = producer
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Close implements the Sink interface.
func (p *pulsarSink) Close() error {
	// TODO (jayant): the close methods below do not take a context.
	// Having a context is important for tracing. It may be worthwhile
	// making a patch to the downstream library to utilize contexts
	// properly.
	for _, producer := range p.topicProducers {
		producer.Close()
	}
	// Close() may be called before Dial(), so perform a nil check.
	if p.client != nil {
		p.client.Close()
	}
	return nil
}

// Pulsar uses the ordering key to construct batches such that keys
// with the same ordering key are placed in the same batch.
func (p *pulsarSink) orderingKey(key []byte) (string, error) {
	p.hasher.Reset()
	_, err := p.hasher.Write(key)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(int(p.hasher.Sum32() % p.parallelism)), nil
}

// EmitRow implements the Sink interface.
func (p *pulsarSink) EmitRow(
	ctx context.Context,
	td TopicDescriptor,
	key []byte,
	value []byte,
	updated hlc.Timestamp,
	mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	// TODO(jayant): cache the encoded topics to save an alloc
	// TODO(#118863): support updated, mvcc, topic_prefix etc.
	topicName, err := p.topicNamer.Name(td)
	if err != nil {
		return err
	}
	var topicBuffer bytes.Buffer
	json.FromString(topicName).Format(&topicBuffer)

	var content []byte
	switch p.format {
	case changefeedbase.OptFormatJSON:
		var buffer bytes.Buffer
		// Grow all at once to avoid reallocations
		buffer.Grow(26 /* Key/Value/Topic keys */ + len(key) + len(value) + len(topicName))
		buffer.WriteString("{\"Key\":")
		buffer.Write(key)
		buffer.WriteString(",\"Value\":")
		buffer.Write(value)
		buffer.WriteString(",\"Topic\":")
		buffer.Write(topicBuffer.Bytes())
		buffer.WriteString("}")
		content = buffer.Bytes()
	case changefeedbase.OptFormatCSV:
		content = value
	default:
		return errors.AssertionFailedf("unsupported format %s", p.format)
	}

	orderingKey, err := p.orderingKey(key)
	if err != nil {
		return err
	}

	msg := &pulsar.ProducerMessage{
		Payload:     content,
		OrderingKey: orderingKey,
	}

	if _, ok := p.topicProducers[topicName]; !ok {
		p.topicProducers[topicName], err = p.client.CreateProducer(pulsar.ProducerOptions{
			Topic:              topicName,
			BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
		})
		if err != nil {
			return err
		}
	}
	producer := p.topicProducers[topicName]
	producer.SendAsync(ctx, msg, p.msgCallback(ctx, alloc, mvcc))

	return p.checkError()
}

func (p *pulsarSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	// Flush the buffered rows.
	if err = p.Flush(ctx); err != nil {
		return err
	}
	orderingKey, err := p.orderingKey(data)
	if err != nil {
		return err
	}

	// Emit resolved timestamps for each topic.
	err = p.topicNamer.Each(func(topicName string) error {
		if _, ok := p.topicProducers[topicName]; !ok {
			p.topicProducers[topicName], err = p.client.CreateProducer(pulsar.ProducerOptions{
				Topic:              topicName,
				BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
			})
			if err != nil {
				return err
			}
		}
		producer := p.topicProducers[topicName]
		producer.SendAsync(ctx, &pulsar.ProducerMessage{
			Payload:     data,
			OrderingKey: orderingKey,
		}, p.setError)
		return nil
	})
	if err != nil {
		return err
	}

	if err = p.Flush(ctx); err != nil {
		return err
	}
	return nil
}

// Flush implements the Sink interface.
func (p *pulsarSink) Flush(ctx context.Context) error {
	for _, producer := range p.topicProducers {
		// TODO(#118871): The flush implementation does not respect context cancellation.
		// Once https://github.com/apache/pulsar-client-go/pull/1165 is released,
		// the library should be updated and the context should be plugged in.
		//
		// TODO(#118862): sized based flushes should be implemented, with metrics
		if err := producer.Flush(); err != nil {
			return err
		}
	}
	return p.checkError()
}

// msgCallback releases the alloc associated with a message and
// records errors.
func (p *pulsarSink) msgCallback(
	ctx context.Context, a kvevent.Alloc, mvcc hlc.Timestamp,
) func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
	// Since we cannot distinguish between time spent buffering inside the
	// pulsar client and time spent sending the message, we just set the value
	// of DownstreamClientSend equal to BatchHistNanos.
	sendCb := p.metrics.timers().DownstreamClientSend.Start()
	oneMsgCb := p.metrics.recordOneMessage()

	return func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
		sendCb()
		if err == nil {
			oneMsgCb(mvcc, len(message.Payload), len(message.Payload))
		} else {
			p.setError(id, message, err)
		}
		a.Release(ctx)
	}
}

// setError sets an error.
func (p *pulsarSink) setError(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
	if err == nil {
		return
	}
	err = errors.Wrapf(err, "failed to send message %d for payload %s", id, message.Payload)
	select {
	case p.errCh <- err:
	default:
	}
}

// checkError checks for an error.
func (p *pulsarSink) checkError() error {
	select {
	case err := <-p.errCh:
		return err
	default:
		return nil
	}
}

func makePulsarSink(
	// TODO(jayant): save this context and add logging
	_ context.Context,
	u *changefeedbase.SinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	targets changefeedbase.Targets,
	// TODO(#118862): configure the batching config
	jsonStr changefeedbase.SinkSpecificJSONConfig,
	// TODO(jayant): using the pulsar sink should be version gated, as changefeed
	// job records are persisted and may be read by un-upgraded nodes.
	settings *cluster.Settings,
	mb metricsRecorderBuilder,
	knobs *TestingKnobs,
) (Sink, error) {
	// TODO(#118858): configure auth and validate URL query params
	unsupportedParams := []string{changefeedbase.SinkParamTopicPrefix, changefeedbase.SinkParamTopicName, changefeedbase.SinkParamSchemaTopic}
	for _, param := range unsupportedParams {
		if u.ConsumeParam(param) != "" {
			return nil, unimplemented.NewWithIssuef(118863, "%s is not yet supported", param)
		}
	}

	topicNamer, err := MakeTopicNamer(targets)
	if err != nil {
		return nil, err
	}

	sink := &pulsarSink{
		format: encodingOpts.Format,
		// TODO (jayant): make parallelism configurable
		parallelism:    10,
		hasher:         crc32.New(crc32.MakeTable(crc32.IEEE)),
		topicNamer:     topicNamer,
		metrics:        mb(requiresResourceAccounting),
		knobs:          knobs,
		clientURL:      u.String(),
		errCh:          make(chan error, 1),
		topicProducers: make(map[string]pulsar.Producer),
	}
	return sink, nil
}

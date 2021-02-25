// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

var (
	defaultOrderingKeyCount = uint32(1024)

	gcpScheme = "gcppubsub"
)

func isPubsubSink(u *url.URL) bool {
	return u.Scheme == gcpScheme
}

type pubsubSinkOptions struct {
	enableOrdering      bool
	maxOrderingKeyCount uint32
}

type pendingResult struct {
	orderingKey string
	result      *pubsub.PublishResult
}

type gcpPubsubDesc struct {
	project string
	topic   string
}

type pubsubSink struct {
	desc    gcpPubsubDesc
	options pubsubSinkOptions

	client *pubsub.Client
	topic  *pubsub.Topic

	// mu protects pendingResults
	mu syncutil.Mutex

	pendingResults []pendingResult
}

var _ Sink = (*pubsubSink)(nil)

func (g gcpPubsubDesc) String() string {
	return fmt.Sprintf("projects/%s/topics/%s", g.project, g.topic)
}

var (
	fullTopicPathRE  = regexp.MustCompile("^projects/[^/]+/topics/[^/]+$")
	shortTopicPathRE = regexp.MustCompile("^[^/]+/[^/]+$")
)

// Accepts topic URLs in the same format that the go-cloud library
// does:
//
// gcppubsub://projects/PROJECT_NAME/topics/TOPIC_NAME
// gcppubsub://PROJECT/TOPIC_NAME
//
func parseGCPURL(u *url.URL) (gcpPubsubDesc, error) {
	fullPath := path.Join(u.Host, u.Path)
	if fullTopicPathRE.MatchString(fullPath) {
		parts := strings.SplitN("/", fullPath, 4)
		if len(parts) < 4 {
			return gcpPubsubDesc{}, errors.Errorf("unexpected number of components in %s", fullPath)
		}
		return gcpPubsubDesc{
			project: parts[1],
			topic:   parts[3],
		}, nil
	} else if shortTopicPathRE.MatchString(fullPath) {
		return gcpPubsubDesc{
			project: u.Host,
			topic:   strings.TrimPrefix(u.Path, "/"),
		}, nil
	}
	return gcpPubsubDesc{}, errors.Errorf("could not parse project and topic from %s", fullPath)
}

func makePubsubSink(baseURI string, opts map[string]string) (Sink, error) {
	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case changefeedbase.OptFormatJSON:
	default:
		return nil, errors.Errorf("this sink is incompatible with %s=%s",
			changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}

	switch changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) {
	case changefeedbase.OptEnvelopeWrapped:
	default:
		return nil, errors.Errorf("this sink is incompatible with %s=%s",
			changefeedbase.OptEnvelope, opts[changefeedbase.OptEnvelope])
	}

	// TODO(ssd): Does it? We could put the key in metadata.
	if _, ok := opts[changefeedbase.OptKeyInValue]; !ok {
		return nil, errors.Errorf("this sink requires the WITH %s option", changefeedbase.OptKeyInValue)
	}

	ctx := context.TODO()

	u, err := url.Parse(baseURI)
	if err != nil {
		return nil, errors.Wrap(err, "bad pubsub URL")
	}

	sink := &pubsubSink{
		options: pubsubSinkOptions{
			maxOrderingKeyCount: defaultOrderingKeyCount,
		},
		// TODO(ssd): allocate this based on
		// configurable parameters?
		pendingResults: make([]pendingResult, 0, 32),
	}

	switch u.Scheme {
	case gcpScheme:
		// TODO(ssd): Ensure we provide consistent options
		// between the GCS auth options and these auth
		// options.
		const CredentialsParam = "CREDENTIALS"
		var creds *google.Credentials
		var err error
		q := u.Query()
		clientOpts := []option.ClientOption{}
		if credsBase64 := q.Get(CredentialsParam); credsBase64 != "" {
			q.Del(CredentialsParam)
			credsJSON, err := base64.StdEncoding.DecodeString(credsBase64)
			if err != nil {
				return nil, errors.Wrapf(err, "decoding value of %s", CredentialsParam)
			}
			creds, err = google.CredentialsFromJSON(ctx, credsJSON)
			if err != nil {
				return nil, errors.Wrapf(err, "decoding credentials json")
			}
			clientOpts = append(clientOpts, option.WithTokenSource(creds.TokenSource))
		}

		var hasEndpoint bool
		if endpoint := q.Get(changefeedbase.SinkParamGCPEndpoint); endpoint != "" {
			hasEndpoint = true
			clientOpts = append(clientOpts, option.WithEndpoint(endpoint))
		}

		if enableOrdering := q.Get(changefeedbase.SinkParamEnableOrdering); enableOrdering != "" {
			enableOrdering, err := strconv.ParseBool(enableOrdering)
			if err != nil {
				return nil, errors.Wrapf(err, `param %s must be a bool:`, changefeedbase.SinkParamEnableOrdering)
			}
			if enableOrdering && !hasEndpoint {
				return nil, errors.New("gcp_endpoint must be specified if enable_ordering=true")
			}
			sink.options.enableOrdering = enableOrdering
		}

		if maxOrderingKeyCount := q.Get(changefeedbase.SinkParamMaxOrderingKeyCount); maxOrderingKeyCount != "" {
			max, err := strconv.ParseUint(maxOrderingKeyCount, 10, 32)
			if err != nil {
				return nil, errors.New("max_ordering_key_count must be a 32-bit unsigned integer")
			}
			sink.options.maxOrderingKeyCount = uint32(max)
		}

		if _, ok := opts[changefeedbase.OptResolvedTimestamps]; ok {
			if !sink.options.enableOrdering || sink.options.maxOrderingKeyCount == 0 {
				return nil, errors.New("resolved timestamps require enable_ordering=true and a non-zero max_ordering_key_count")
			}
		}

		pubsubDesc, err := parseGCPURL(u)
		if err != nil {
			return nil, err
		}

		client, err := pubsub.NewClient(ctx, pubsubDesc.project, clientOpts...)
		if err != nil {
			return nil, errors.Wrap(err, "client")
		}

		sink.desc = pubsubDesc
		sink.topic = client.Topic(pubsubDesc.topic)
		sink.topic.EnableMessageOrdering = sink.options.enableOrdering
	default:
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}
	return sink, nil
}

// orderingKey returns a string that can be used as a GCP OrderingKey.
//
// The orderingKey choice interacts with the underlying libraries
// batching/bundling implementation:
//
// (1) Since all messages in a given bulk call to GCP need to share an
// ordering key. Thus, if we use per-row ordering keys, we won't
// achieve much batching unless we are under a high write load load.
//
// (2) The batching code creates a bundler for each ordering key we
// publish and doesn't appear to clean them up for the lifetime of the
// topic.
//
// Ordering keys are optionally limited to at most
// p.options.maxOrderingKeyCount unique values.
func (p *pubsubSink) orderingKey(key []byte) string {
	h := fnv.New32()
	h.Write(key)
	orderBucket := h.Sum32()
	if p.options.maxOrderingKeyCount > 0 {
		orderBucket = orderBucket % p.options.maxOrderingKeyCount
	}
	return fmt.Sprintf("orderkey-%d", orderBucket)
}

func (p *pubsubSink) EmitRow(
	ctx context.Context, topic TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	msg := &pubsub.Message{
		Attributes: map[string]string{
			"topic": topic.GetName(),
		},
		Data: value,
	}
	if p.options.enableOrdering {
		msg.OrderingKey = p.orderingKey(key)
	}
	return errors.Wrap(p.emitMessage(ctx, msg), "failed to emit row")
}

func (p *pubsubSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(ctx, noTopic, resolved)
	if err != nil {
		return err
	}

	if p.options.enableOrdering && p.options.maxOrderingKeyCount > 0 {
		for i := uint32(0); i < p.options.maxOrderingKeyCount; i++ {
			err := p.emitMessage(ctx, &pubsub.Message{
				Data:        payload,
				OrderingKey: fmt.Sprintf("orderkey-%d", i),
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
	return p.emitMessage(ctx, &pubsub.Message{Data: payload})
}

func (p *pubsubSink) emitMessage(ctx context.Context, msg *pubsub.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	resp := p.topic.Publish(ctx, msg)
	p.pendingResults = append(p.pendingResults, pendingResult{
		orderingKey: msg.OrderingKey,
		result:      resp,
	})
	return nil
}

func (p *pubsubSink) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	failedOrderingKeys := []string{}
	// We wait for all pending results, collecting all failures.
	//
	// The underlying library may still be bundling messages, but
	// in its default configuration, won't wait more than a
	// configurable delay threshold (by default 10ms).
	//
	// TODO(ssd): Do we want to collect errors?
	var firstErr error
	for _, pendingRes := range p.pendingResults {
		_, err := pendingRes.result.Get(ctx)
		if err != nil {
			if firstErr == nil {
				log.Warningf(ctx, "failed to publish message: %s", err.Error())
				firstErr = err
			}

			if pendingRes.orderingKey != "" {
				failedOrderingKeys = append(failedOrderingKeys, pendingRes.orderingKey)
			}
		}
	}

	// TODO(ssd): We should prove to ourselves that
	// this is OK. The library pauses publishing on any ordering
	// key that has had a failure, to prevent unexpected ordering
	// issues.
	for _, key := range failedOrderingKeys {
		p.topic.ResumePublish(key)
	}

	// TODO(ssd): manage memory growth here. Perhaps throw away
	// the slice and reallocate if it gets too big?
	p.pendingResults = p.pendingResults[:0]
	return firstErr
}

func (p *pubsubSink) Close() error {
	if p.topic != nil {
		p.topic.Stop()
	}
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"time"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	pb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const credentialsParam = "CREDENTIALS"

// GcpScheme to be used in testfeed and sink.go
const GcpScheme = "gcpubsub"
const gcpScope = "https://www.googleapis.com/auth/pubsub"
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
const globalGCPEndpoint = "pubsub.googleapis.com:443"

// isPubsubSink returns true if url contains scheme with valid pubsub sink
func isPubsubSink(u *url.URL) bool {
	return u.Scheme == GcpScheme
}

type pubsubSinkClient struct {
	ctx       context.Context
	client    *pubsub.PublisherClient
	projectID string
	format    changefeedbase.FormatType
	batchCfg  sinkBatchConfig
	mu        struct {
		syncutil.RWMutex

		// Topic creation errors may not be an actual issue unless the Publish call
		// itself fails, so creation errors are stored for future use in the event of
		// a publish error.
		topicCreateErr error

		// Caches whether or not we've already created a topic
		topicCache map[string]struct{}
	}
}

var _ SinkClient = (*pubsubSinkClient)(nil)
var _ SinkPayload = (*pb.PubsubMessage)(nil)

func makePubsubSinkClient(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	targets changefeedbase.Targets,
	batchCfg sinkBatchConfig,
	unordered bool,
	knobs *TestingKnobs,
) (SinkClient, error) {
	if u.Scheme != GcpScheme {
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}

	var formatType changefeedbase.FormatType
	switch encodingOpts.Format {
	case changefeedbase.OptFormatJSON:
		formatType = changefeedbase.OptFormatJSON
	case changefeedbase.OptFormatCSV:
		formatType = changefeedbase.OptFormatCSV
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, encodingOpts.Format)
	}

	switch encodingOpts.Envelope {
	case changefeedbase.OptEnvelopeWrapped, changefeedbase.OptEnvelopeBare:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, encodingOpts.Envelope)
	}

	pubsubURL := sinkURL{URL: u, q: u.Query()}

	projectID := pubsubURL.Host
	if projectID == "" {
		return nil, errors.New("missing project name")
	}

	var err error
	var publisherClient *pubsub.PublisherClient

	// In unit tests the publisherClient gets set immediately after initializing
	// the sink object via knobs.WrapSink.
	if knobs == nil || !knobs.PubsubClientSkipClientCreation {
		publisherClient, err = makePublisherClient(ctx, pubsubURL, unordered)
		if err != nil {
			return nil, err
		}
	}

	sinkClient := &pubsubSinkClient{
		ctx:       ctx,
		format:    formatType,
		client:    publisherClient,
		batchCfg:  batchCfg,
		projectID: projectID,
	}
	sinkClient.mu.topicCache = make(map[string]struct{})

	return sinkClient, nil
}

// MakeResolvedPayload implements the SinkClient interface
func (sc *pubsubSinkClient) MakeResolvedPayload(body []byte, topic string) (SinkPayload, error) {
	return &pb.PublishRequest{
		Topic: sc.gcPubsubTopic(topic),
		Messages: []*pb.PubsubMessage{{
			Data: body,
		}},
	}, nil
}

func (sc *pubsubSinkClient) maybeCreateTopic(topic string) error {
	sc.mu.RLock()
	_, ok := sc.mu.topicCache[topic]
	if ok {
		sc.mu.RUnlock()
		return nil
	}
	sc.mu.RUnlock()
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, ok = sc.mu.topicCache[topic]
	if ok {
		return nil
	}

	_, err := sc.client.CreateTopic(sc.ctx, &pb.Topic{Name: topic})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		if status.Code(err) == codes.PermissionDenied {
			// PermissionDenied may not be fatal if the topic already exists,
			// but record it in case it turns out not to.
			sc.mu.topicCreateErr = err
		} else {
			sc.mu.topicCreateErr = err
			return err
		}
	}
	sc.mu.topicCache[topic] = struct{}{}
	return nil
}

// Flush implements the SinkClient interface
func (sc *pubsubSinkClient) Flush(ctx context.Context, payload SinkPayload) error {
	publishRequest := payload.(*pb.PublishRequest)

	err := sc.maybeCreateTopic(publishRequest.Topic)
	if err != nil {
		return err
	}

	_, err = sc.client.Publish(sc.ctx, publishRequest)

	if status.Code(err) == codes.NotFound {
		sc.mu.RLock()
		defer sc.mu.RUnlock()
		if sc.mu.topicCreateErr != nil {
			return errors.WithHint(
				errors.Wrap(sc.mu.topicCreateErr,
					"Topic not found, and attempt to autocreate it failed."),
				"Create topics in advance or grant this service account the pubsub.editor role on your project.")
		}
	}
	return err
}

type pubsubBuffer struct {
	sc           *pubsubSinkClient
	topic        string
	topicEncoded []byte
	messages     []*pb.PubsubMessage
	numBytes     int
}

var _ BatchBuffer = (*pubsubBuffer)(nil)

// Append implements the BatchBuffer interface
func (psb *pubsubBuffer) Append(key []byte, value []byte) {
	var content []byte
	switch psb.sc.format {
	case changefeedbase.OptFormatJSON:
		var buffer bytes.Buffer
		// Grow all at once to avoid reallocations
		buffer.Grow(26 /* Key/Value/Topic keys */ + len(key) + len(value) + len(psb.topicEncoded))
		buffer.WriteString("{\"Key\":")
		buffer.Write(key)
		buffer.WriteString(",\"Value\":")
		buffer.Write(value)
		buffer.WriteString(",\"Topic\":")
		buffer.Write(psb.topicEncoded)
		buffer.WriteString("}")
		content = buffer.Bytes()
	case changefeedbase.OptFormatCSV:
		content = value
	}

	psb.messages = append(psb.messages, &pb.PubsubMessage{Data: content})
	psb.numBytes += len(content)
}

// Close implements the BatchBuffer interface
func (psb *pubsubBuffer) Close() (SinkPayload, error) {
	return &pb.PublishRequest{
		Topic:    psb.sc.gcPubsubTopic(psb.topic),
		Messages: psb.messages,
	}, nil
}

// ShouldFlush implements the BatchBuffer interface
func (psb *pubsubBuffer) ShouldFlush() bool {
	return shouldFlushBatch(psb.numBytes, len(psb.messages), psb.sc.batchCfg)
}

// MakeBatchBuffer implements the SinkClient interface
func (sc *pubsubSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	var topicBuffer bytes.Buffer
	json.FromString(topic).Format(&topicBuffer)
	return &pubsubBuffer{
		sc:           sc,
		topic:        topic,
		topicEncoded: topicBuffer.Bytes(),
		messages:     make([]*pb.PubsubMessage, 0, sc.batchCfg.Messages),
	}
}

// Close implements the SinkClient interface
func (pe *pubsubSinkClient) Close() error {
	return pe.client.Close()
}

func makePublisherClient(
	ctx context.Context, url sinkURL, unordered bool,
) (*pubsub.PublisherClient, error) {
	const regionParam = "region"
	region := url.consumeParam(regionParam)
	var endpoint string
	if region == "" {
		if unordered {
			endpoint = globalGCPEndpoint
		} else {
			return nil, errors.WithHintf(errors.New("region query parameter not found"),
				"Use of gcpubsub without specifying a region requires the WITH %s option.",
				changefeedbase.OptUnordered)
		}
	} else {
		endpoint = gcpEndpointForRegion(region)
	}

	creds, err := getGCPCredentials(ctx, url)
	if err != nil {
		return nil, err
	}

	client, err := pubsub.NewPublisherClient(
		ctx,
		option.WithEndpoint(endpoint),
		creds,
	)
	if err != nil {
		return nil, errors.Wrap(err, "opening client")
	}

	return client, nil
}

// Generate the cloud endpoint that's specific to a region (e.g. us-east1).
// Ideally this would be discoverable via API but doesn't seem to be.
// A hardcoded approach looks to be correct right now.
func gcpEndpointForRegion(region string) string {
	return fmt.Sprintf("%s-pubsub.googleapis.com:443", region)
}

// TODO: unify gcp credentials code with gcp cloud storage credentials code
// getGCPCredentials returns gcp credentials parsed out from url
func getGCPCredentials(ctx context.Context, u sinkURL) (option.ClientOption, error) {
	const authParam = "AUTH"
	const assumeRoleParam = "ASSUME_ROLE"
	const authSpecified = "specified"
	const authImplicit = "implicit"
	const authDefault = "default"

	var credsJSON []byte
	var creds *google.Credentials
	var err error
	authOption := u.consumeParam(authParam)
	assumeRoleOption := u.consumeParam(assumeRoleParam)
	authScope := gcpScope
	if assumeRoleOption != "" {
		// If we need to assume a role, the credentials need to have the scope to
		// impersonate instead.
		authScope = cloudPlatformScope
	}

	// implemented according to https://github.com/cockroachdb/cockroach/pull/64737
	switch authOption {
	case authImplicit:
		creds, err = google.FindDefaultCredentials(ctx, authScope)
		if err != nil {
			return nil, err
		}
	case authSpecified:
		fallthrough
	case authDefault:
		fallthrough
	default:
		if u.q.Get(credentialsParam) == "" {
			return nil, errors.New("missing credentials parameter")
		}
		err := u.decodeBase64(credentialsParam, &credsJSON)
		if err != nil {
			return nil, errors.Wrap(err, "decoding credentials json")
		}
		creds, err = google.CredentialsFromJSON(ctx, credsJSON, authScope)
		if err != nil {
			return nil, errors.Wrap(err, "creating credentials from json")
		}
	}

	credsOpt := option.WithCredentials(creds)
	if assumeRoleOption != "" {
		assumeRole, delegateRoles := cloud.ParseRoleString(assumeRoleOption)
		cfg := impersonate.CredentialsConfig{
			TargetPrincipal: assumeRole,
			Scopes:          []string{gcpScope},
			Delegates:       delegateRoles,
		}

		ts, err := impersonate.CredentialsTokenSource(ctx, cfg, credsOpt)
		if err != nil {
			return nil, errors.Wrap(err, "creating impersonate credentials")
		}
		return option.WithTokenSource(ts), nil
	}

	return credsOpt, nil
}

func (sc *pubsubSinkClient) gcPubsubTopic(topic string) string {
	return fmt.Sprintf("projects/%s/topics/%s", sc.projectID, topic)
}

func makePubsubSink(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	jsonConfig changefeedbase.SinkSpecificJSONConfig,
	targets changefeedbase.Targets,
	unordered bool,
	parallelism int,
	pacerFactory func() *admission.Pacer,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
	knobs *TestingKnobs,
) (Sink, error) {
	batchCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig, sinkJSONConfig{
		// GCPubsub library defaults
		Flush: sinkBatchConfig{
			Frequency: jsonDuration(10 * time.Millisecond),
			Messages:  100,
			Bytes:     1e6,
		},
	})
	if err != nil {
		return nil, err
	}

	sinkClient, err := makePubsubSinkClient(ctx, u, encodingOpts, targets, batchCfg, unordered, knobs)
	if err != nil {
		return nil, err
	}

	pubsubURL := sinkURL{URL: u, q: u.Query()}
	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)
	topicNamer, err := MakeTopicNamer(targets, WithSingleName(pubsubTopicName))
	if err != nil {
		return nil, err
	}

	return makeBatchingSink(
		ctx,
		sinkTypePubsub,
		sinkClient,
		time.Duration(batchCfg.Frequency),
		retryOpts,
		parallelism,
		topicNamer,
		pacerFactory,
		source,
		mb(requiresResourceAccounting),
	), nil
}

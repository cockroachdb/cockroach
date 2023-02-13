// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const credentialsParam = "CREDENTIALS"

// GcpScheme to be used in testfeed and sink.go
const GcpScheme = "gcpubsub"
const gcpScope = "https://www.googleapis.com/auth/pubsub"
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// isPubsubSink returns true if url contains scheme with valid pubsub sink
func isPubsubSink(u *url.URL) bool {
	return u.Scheme == GcpScheme
}

type pubsubSinkClient struct {
	ctx       context.Context
	client    *pubsub.PublisherClient
	projectID string
	format    changefeedbase.FormatType
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

// EncodeBatch implements the SinkClient interface
func (pe *pubsubSinkClient) EncodeBatch(topic string, msgs []messagePayload) (SinkPayload, error) {
	pbsbMsgs := make([]*pb.PubsubMessage, len(msgs))
	for i, msg := range msgs {
		var content []byte
		var err error
		switch pe.format {
		case changefeedbase.OptFormatJSON:
			content, err = json.Marshal(jsonPayload{
				Key:   msg.key,
				Value: msg.val,
				Topic: msg.topic,
			})
			if err != nil {
				return nil, err
			}
		case changefeedbase.OptFormatCSV:
			content = msg.val
		}

		pbsbMsgs[i] = &pb.PubsubMessage{
			Data: content,
		}
	}

	req := &pb.PublishRequest{
		Topic:    pe.gcPubsubTopic(topic),
		Messages: pbsbMsgs,
	}

	return req, nil
}

// EncodeResolvedMeessage implements the SinkClient interface
func (pe *pubsubSinkClient) EncodeResolvedMessage(
	payload resolvedMessagePayload,
) (SinkPayload, error) {
	return &pb.PublishRequest{
		Topic: pe.gcPubsubTopic(payload.topic),
		Messages: []*pb.PubsubMessage{{
			Data: payload.body,
		}},
	}, nil
}

func (pe *pubsubSinkClient) gcPubsubTopic(topic string) string {
	return fmt.Sprintf("projects/%s/topics/%s", pe.projectID, topic)
}

func (pe *pubsubSinkClient) maybeCreateTopic(topic string) error {
	pe.mu.RLock()
	_, ok := pe.mu.topicCache[topic]
	if ok {
		pe.mu.RUnlock()
		return nil
	}
	pe.mu.RUnlock()
	pe.mu.Lock()
	defer pe.mu.Unlock()
	_, ok = pe.mu.topicCache[topic]
	if ok {
		return nil
	}

	_, err := pe.client.CreateTopic(pe.ctx, &pb.Topic{Name: topic})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		if status.Code(err) == codes.PermissionDenied {
			// PermissionDenied may not be fatal if the topic already exists,
			// but record it in case it turns out not to.
			pe.mu.topicCreateErr = err
		} else {
			pe.mu.topicCreateErr = err
			return err
		}
	}
	pe.mu.topicCache[topic] = struct{}{}
	return nil
}

// EmitPayload implements the SinkClient interface
func (pe *pubsubSinkClient) EmitPayload(payload SinkPayload) error {
	publishRequest, ok := payload.(*pb.PublishRequest)
	if !ok {
		return errors.Errorf("cannot construct pubsub payload from given sinkPayload")
	}

	err := pe.maybeCreateTopic(publishRequest.Topic)
	if err != nil {
		return err
	}

	_, err = pe.client.Publish(pe.ctx, publishRequest)

	if status.Code(err) == codes.NotFound {
		pe.mu.RLock()
		defer pe.mu.RUnlock()
		if pe.mu.topicCreateErr != nil {
			return errors.WithHint(
				errors.Wrap(pe.mu.topicCreateErr,
					"Topic not found, and attempt to autocreate it failed."),
				"Create topics in advance or grant this service account the pubsub.editor role on your project.")
		}
	}
	return err
}

// Close implements the SinkClient interface
func (pe *pubsubSinkClient) Close() error {
	return pe.client.Close()
}

func makePubsubSinkClient(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	targets changefeedbase.Targets,
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

	publisherClient, err := makePublisherClient(ctx, pubsubURL, knobs)
	if err != nil {
		return nil, err
	}

	sinkClient := &pubsubSinkClient{
		ctx:       ctx,
		format:    formatType,
		client:    publisherClient,
		projectID: projectID,
	}
	sinkClient.mu.topicCache = make(map[string]struct{})

	return sinkClient, nil
}

func makePublisherClient(
	ctx context.Context, url sinkURL, knobs *TestingKnobs,
) (*pubsub.PublisherClient, error) {
	const regionParam = "region"
	region := url.consumeParam(regionParam)
	if region == "" {
		return nil, errors.New("region query parameter not found")
	}

	options := []option.ClientOption{
		option.WithEndpoint(gcpEndpointForRegion(region)),
	}

	if knobs == nil || !knobs.PubsubClientSkipCredentialsCheck {
		creds, err := getGCPCredentials(ctx, url)
		if err != nil {
			return nil, err
		}
		options = append(options, creds)
	}

	client, err := pubsub.NewPublisherClient(
		ctx,
		options...,
	)
	if err != nil {
		return nil, errors.Wrap(err, "opening client")
	}

	return client, nil
}

func makePubsubSink(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	jsonConfig changefeedbase.SinkSpecificJSONConfig,
	targets changefeedbase.Targets,
	numWorkers int64,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
	knobs *TestingKnobs,
	pacer SinkPacer,
) (Sink, error) {
	sinkClient, err := makePubsubSinkClient(ctx, u, encodingOpts, targets, knobs)
	if err != nil {
		return nil, err
	}

	flushCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig, sinkJSONConfig{
		// GCPubsub defaults
		Flush: sinkBatchConfig{
			Frequency: jsonDuration(10 * time.Millisecond),
			Messages:  100,
			Bytes:     1e6,
		},
	})
	if err != nil {
		return nil, err
	}

	pubsubURL := sinkURL{URL: u, q: u.Query()}
	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)
	topicNamer, err := MakeTopicNamer(targets, WithSingleName(pubsubTopicName))
	if err != nil {
		return nil, err
	}

	return makeParallelBatchingSink(
		ctx,
		sinkTypePubsub,
		sinkClient,
		flushCfg,
		retryOpts,
		numWorkers,
		topicNamer,
		source,
		mb(requiresResourceAccounting),
		pacer,
	), nil
}

// Generate the cloud endpoint that's specific to a region (e.g. us-east1).
// Ideally this would be discoverable via API but doesn't seem to be.
// A hardcoded approach looks to be correct right now.
func gcpEndpointForRegion(region string) string {
	return fmt.Sprintf("%s-pubsub.googleapis.com:443", region)
}

type jsonPayload struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
	Topic string          `json:"topic"`
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

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	encjson "encoding/json"
	"fmt"
	"net"
	"net/url"
	"time"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	pb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const credentialsParam = "CREDENTIALS"

// GcpScheme to be used in testfeed and sink.go
const GcpScheme = "gcpubsub"
const gcpScope = "https://www.googleapis.com/auth/pubsub"
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
const globalGCPEndpoint = "pubsub.googleapis.com:443"

type jsonPayload struct {
	Key   encjson.RawMessage `json:"key"`
	Value encjson.RawMessage `json:"value"`
	Topic string             `json:"topic"`
}

// isPubsubSink returns true if url contains scheme with valid pubsub sink
func isPubsubSink(u *url.URL) bool {
	return u.Scheme == GcpScheme
}

type pubsubSinkClient struct {
	ctx                    context.Context
	client                 *pubsub.PublisherClient
	projectID              string
	format                 changefeedbase.FormatType
	batchCfg               sinkBatchConfig
	withTableNameAttribute bool
	mu                     struct {
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
	withTableNameAttribute bool,
	knobs *TestingKnobs,
	m metricsRecorder,
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
	case changefeedbase.OptEnvelopeWrapped, changefeedbase.OptEnvelopeBare, changefeedbase.OptEnvelopeEnriched:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, encodingOpts.Envelope)
	}

	pubsubURL := &changefeedbase.SinkURL{URL: u}

	projectID := pubsubURL.Host
	if projectID == "" {
		return nil, errors.New("missing project name")
	}

	var err error
	var publisherClient *pubsub.PublisherClient

	// In unit tests the publisherClient gets set immediately after initializing
	// the sink object via knobs.WrapSink.
	if knobs == nil || !knobs.PubsubClientSkipClientCreation {
		publisherClient, err = makePublisherClient(ctx, pubsubURL, unordered, m.netMetrics())
		if err != nil {
			return nil, err
		}
	}

	sinkClient := &pubsubSinkClient{
		ctx:                    ctx,
		format:                 formatType,
		client:                 publisherClient,
		batchCfg:               batchCfg,
		projectID:              projectID,
		withTableNameAttribute: withTableNameAttribute,
	}
	sinkClient.mu.topicCache = make(map[string]struct{})

	return sinkClient, nil
}

// FlushResolvedPayload implements the SinkClient interface.
func (sc *pubsubSinkClient) FlushResolvedPayload(
	ctx context.Context,
	body []byte,
	forEachTopic func(func(topic string) error) error,
	retryOpts retry.Options,
) error {
	return forEachTopic(func(topic string) error {
		pl := &pb.PublishRequest{
			Topic: sc.gcPubsubTopic(topic),
			Messages: []*pb.PubsubMessage{{
				Data: body,
			}},
		}
		return retry.WithMaxAttempts(ctx, retryOpts, retryOpts.MaxRetries+1, func() error {
			return sc.Flush(ctx, pl)
		})
	})
}

func (sc *pubsubSinkClient) CheckConnection(ctx context.Context) error {
	return nil
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
	// Cache for attributes which are sent along with each message.
	// This lets us re-use expensive map allocs for messages in the batch
	// with the same attributes.
	attributesCache map[attributes]map[string]string
}

var _ BatchBuffer = (*pubsubBuffer)(nil)

// Append implements the BatchBuffer interface
func (psb *pubsubBuffer) Append(key []byte, value []byte, attributes attributes) {
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

	msg := &pb.PubsubMessage{Data: content}
	if psb.sc.withTableNameAttribute {
		if _, ok := psb.attributesCache[attributes]; !ok {
			psb.attributesCache[attributes] = map[string]string{"TABLE_NAME": attributes.tableName}
		}
		msg.Attributes = psb.attributesCache[attributes]
	}

	psb.messages = append(psb.messages, msg)
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
	psb := &pubsubBuffer{
		sc:           sc,
		topic:        topic,
		topicEncoded: topicBuffer.Bytes(),
		messages:     make([]*pb.PubsubMessage, 0, sc.batchCfg.Messages),
	}
	if sc.withTableNameAttribute {
		psb.attributesCache = make(map[attributes]map[string]string)
	}
	return psb
}

// Close implements the SinkClient interface
func (pe *pubsubSinkClient) Close() error {
	return pe.client.Close()
}

func makePublisherClient(
	ctx context.Context, url *changefeedbase.SinkURL, unordered bool, nm *cidr.NetMetrics,
) (*pubsub.PublisherClient, error) {
	const regionParam = "region"
	region := url.ConsumeParam(regionParam)
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

	// Set up the network metrics for tracking bytes in/out.
	dialContext := nm.Wrap((&net.Dialer{}).DialContext, "pubsub")
	dial := func(ctx context.Context, target string) (net.Conn, error) {
		return dialContext(ctx, "tcp", target)
	}
	opts := []option.ClientOption{creds, option.WithEndpoint(endpoint), option.WithGRPCDialOption(grpc.WithContextDialer(dial))}

	// See https://pkg.go.dev/cloud.google.com/go/pubsub#hdr-Emulator for emulator information.
	if addr, _ := envutil.ExternalEnvString("PUBSUB_EMULATOR_HOST", 1); addr != "" {
		log.Infof(ctx, "Establishing connection to pubsub emulator at %s", addr)
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, errors.Newf("grpc.Dial: %w", err)
		}
		opts = append(opts, option.WithGRPCConn(conn), option.WithTelemetryDisabled())
	}

	client, err := pubsub.NewPublisherClient(ctx, opts...)
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
func getGCPCredentials(
	ctx context.Context, u *changefeedbase.SinkURL,
) (option.ClientOption, error) {
	const authParam = "AUTH"
	const assumeRoleParam = "ASSUME_ROLE"
	const authSpecified = "specified"
	const authImplicit = "implicit"
	const authDefault = "default"

	var credsJSON []byte
	var creds *google.Credentials
	var err error
	authOption := u.ConsumeParam(authParam)
	assumeRoleOption := u.ConsumeParam(assumeRoleParam)
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
		if u.PeekParam(credentialsParam) == "" {
			return nil, errors.New("missing credentials parameter")
		}
		err := u.DecodeBase64(credentialsParam, &credsJSON)
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
	settings *cluster.Settings,
	knobs *TestingKnobs,
) (Sink, error) {
	m := mb(requiresResourceAccounting)

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

	pubsubURL := &changefeedbase.SinkURL{URL: u}
	var includeTableNameAttribute bool
	_, err = pubsubURL.ConsumeBool(changefeedbase.SinkParamTableNameAttribute, &includeTableNameAttribute)
	if err != nil {
		return nil, err
	}
	sinkClient, err := makePubsubSinkClient(ctx, u, encodingOpts, targets, batchCfg, unordered,
		includeTableNameAttribute, knobs, m)
	if err != nil {
		return nil, err
	}

	pubsubTopicName := pubsubURL.ConsumeParam(changefeedbase.SinkParamTopicName)
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
		m,
		settings,
	), nil
}

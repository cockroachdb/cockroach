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
	"hash/crc32"
	"net/url"

	"cloud.google.com/go/pubsub"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

// TODO: make numOfWorkers configurable
const numOfWorkers = 128

// isPubsubSInk returns true if url contains scheme with valid pubsub sink
func isPubsubSink(u *url.URL) bool {
	return u.Scheme == GcpScheme
}

type pubsubClient interface {
	init() error
	closeTopics()
	flushTopics()
	sendMessage(content []byte, topic string, key string) error
	sendMessageToAllTopics(content []byte) error
	connectivityErrorLocked() error
}

type jsonPayload struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
	Topic string          `json:"topic"`
}

// payload struct is sent to the sink
type payload struct {
	Key   []byte
	Value []byte
	Topic string
}

// pubsubMessage is sent to worker channels for workers to consume
type pubsubMessage struct {
	alloc   kvevent.Alloc
	message payload
	isFlush bool
}

type gcpPubsubClient struct {
	client     *pubsub.Client
	ctx        context.Context
	projectID  string
	endpoint   string
	topicNamer *TopicNamer
	url        sinkURL

	mu struct {
		syncutil.Mutex
		autocreateError error
		publishError    error
		topics          map[string]*pubsub.Topic
	}
}

type pubsubSink struct {
	numWorkers int

	workerCtx   context.Context
	workerGroup ctxgroup.Group

	exitWorkers func()               // Signaled to shut down all workers.
	eventsChans []chan pubsubMessage //channel where messages are consumed and sent out

	// flushDone channel signaled when flushing completes.
	flushDone chan struct{}

	// errChan is written to indicate an error while sending message.
	errChan chan error

	client     pubsubClient
	topicNamer *TopicNamer

	format changefeedbase.FormatType
}

func (p *pubsubSink) getConcreteType() sinkType {
	return sinkTypePubsub
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

// MakePubsubSink returns the corresponding pubsub sink based on the url given
func MakePubsubSink(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	targets changefeedbase.Targets,
	unordered bool,
) (Sink, error) {

	pubsubURL := sinkURL{URL: u, q: u.Query()}
	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)

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

	ctx, cancel := context.WithCancel(ctx)
	p := &pubsubSink{
		workerCtx:   ctx,
		numWorkers:  numOfWorkers,
		exitWorkers: cancel,
		format:      formatType,
	}

	// creates custom pubsub object based on scheme
	switch u.Scheme {
	case GcpScheme:
		const regionParam = "region"
		projectID := pubsubURL.Host
		if projectID == "" {
			return nil, errors.New("missing project name")
		}
		region := pubsubURL.consumeParam(regionParam)
		var endpoint string
		if region == "" {
			if unordered {
				endpoint = globalGCPEndpoint
			} else {
				return nil, errors.WithHintf(errors.New("region query parameter not found"),
					"Using of gcpubsub without specifying a region requires the WITH %s option.",
					changefeedbase.OptUnordered)
			}
		} else {
			endpoint = gcpEndpointForRegion(region)
		}
		tn, err := MakeTopicNamer(targets, WithSingleName(pubsubTopicName))
		if err != nil {
			return nil, err
		}
		g := &gcpPubsubClient{
			topicNamer: tn,
			ctx:        ctx,
			projectID:  projectID,
			endpoint:   endpoint,
			url:        pubsubURL,
		}
		p.client = g
		p.topicNamer = tn
		return p, nil
	default:
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}
}

func (p *pubsubSink) Dial() error {
	p.setupWorkers()
	return p.client.init()
}

// EmitRow pushes a message to event channel where it is consumed by workers
func (p *pubsubSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	topicName, err := p.topicNamer.Name(topic)
	if err != nil {
		return err
	}
	m := pubsubMessage{
		alloc: alloc, isFlush: false, message: payload{
			Key:   key,
			Value: value,
			Topic: topicName,
		}}

	// calculate index by hashing key
	i := p.workerIndex(key)
	select {
	// check the sink context in case workers have been terminated
	case <-p.workerCtx.Done():
		// check again for error in case it triggered since last check
		// will return more verbose error instead of "context canceled"
		return errors.CombineErrors(p.workerCtx.Err(), p.sinkErrorLocked())
	case <-ctx.Done():
		return ctx.Err()
	case err := <-p.errChan:
		return err
	case p.eventsChans[i] <- m:
	}
	return nil
}

// EmitResolvedTimestamp sends resolved timestamp message
func (p *pubsubSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	payload, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return errors.Wrap(err, "encoding resolved timestamp")
	}

	return p.client.sendMessageToAllTopics(payload)
}

// Flush blocks until all messages in the event channels are sent
func (p *pubsubSink) Flush(ctx context.Context) error {
	if err := p.flush(ctx); err != nil {
		return errors.CombineErrors(p.client.connectivityErrorLocked(), err)
	}
	return nil
}

func (p *pubsubSink) flush(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-p.errChan:
		return err
	default:
		err := p.flushWorkers()
		if err != nil {
			return err
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-p.errChan:
		return err
	case <-p.flushDone:
		return p.sinkErrorLocked()
	}

}

// Close closes all the channels and shutdowns the topic
func (p *pubsubSink) Close() error {
	p.client.closeTopics()
	p.exitWorkers()
	_ = p.workerGroup.Wait()
	if p.errChan != nil {
		close(p.errChan)
	}
	if p.flushDone != nil {
		close(p.flushDone)
	}
	for i := 0; i < p.numWorkers; i++ {
		if p.eventsChans[i] != nil {
			close(p.eventsChans[i])
		}
	}
	return nil
}

// Topics gives the names of all topics that have been initialized
// and will receive resolved timestamps.
func (p *pubsubSink) Topics() []string {
	return p.topicNamer.DisplayNamesSlice()
}

func (p *gcpPubsubClient) cacheTopicLocked(name string, topic *pubsub.Topic) {
	//TODO (zinger): Investigate whether changing topics to a sync.Map would be
	//faster here, I think it would.
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.topics[name] = topic
}

func (p *gcpPubsubClient) getTopicLocked(name string) (t *pubsub.Topic, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok = p.mu.topics[name]
	return t, ok
}

func (p *gcpPubsubClient) getTopicClient(name string) (*pubsub.Topic, error) {
	if topic, ok := p.getTopicLocked(name); ok {
		return topic, nil
	}
	topic, err := p.openTopic(name)
	if err != nil {
		return nil, err
	}
	p.cacheTopicLocked(name, topic)
	return topic, nil
}

// setupWorkers sets up the channels used by the sink and starts a goroutine for every worker
func (p *pubsubSink) setupWorkers() {
	// setup events channels to send to workers and the worker group
	p.eventsChans = make([]chan pubsubMessage, p.numWorkers)
	p.workerGroup = ctxgroup.WithContext(p.workerCtx)

	// an error channel with buffer for the first error.
	p.errChan = make(chan error, 1)

	// flushDone notified when flush completes.
	p.flushDone = make(chan struct{}, 1)

	for i := 0; i < p.numWorkers; i++ {
		//initialize worker goroutine and channel for worker
		p.eventsChans[i] = make(chan pubsubMessage)
		j := i
		p.workerGroup.GoCtx(func(ctx context.Context) error {
			p.workerLoop(j)
			return nil
		})
	}
}

// workerLoop consumes any message sent to the channel corresponding to the worker index
func (p *pubsubSink) workerLoop(workerIndex int) {
	for {
		select {
		case <-p.workerCtx.Done():
			return
		case msg := <-p.eventsChans[workerIndex]:
			if msg.isFlush {
				// Signals a flush request, makes sure that the messages in eventsChans are finished sending
				continue
			}

			var content []byte
			var err error
			switch p.format {
			case changefeedbase.OptFormatJSON:
				content, err = json.Marshal(jsonPayload{
					Key:   msg.message.Key,
					Value: msg.message.Value,
					Topic: msg.message.Topic,
				})
				if err != nil {
					p.exitWorkersWithError(err)
				}
			case changefeedbase.OptFormatCSV:
				content = msg.message.Value
			}

			err = p.client.sendMessage(content, msg.message.Topic, string(msg.message.Key))
			if err != nil {
				p.exitWorkersWithError(err)
			}
			msg.alloc.Release(p.workerCtx)
		}
	}
}

// exitWorkersWithError sends an error to the sink error channel
func (p *pubsubSink) exitWorkersWithError(err error) {
	// errChan has buffer size 1, first error will be saved to the buffer and
	// subsequent errors will be ignored
	select {
	case p.errChan <- err:
		p.exitWorkers()
	default:
	}
}

// sinkError checks if there is an error in the error channel
func (p *pubsubSink) sinkErrorLocked() error {
	select {
	case err := <-p.errChan:
		return err
	default:
	}
	return nil
}

// workerIndex hashes key to return a worker index
func (p *pubsubSink) workerIndex(key []byte) uint32 {
	return crc32.ChecksumIEEE(key) % uint32(p.numWorkers)
}

// flushWorkers sends a flush message to every worker channel and then signals sink that flush is done
func (p *pubsubSink) flushWorkers() error {
	for i := 0; i < p.numWorkers; i++ {
		//flush message will be blocked until all the messages in the channel are processed
		select {
		case <-p.workerCtx.Done():
			return p.workerCtx.Err()
		case p.eventsChans[i] <- pubsubMessage{isFlush: true}:
		}
	}

	// flush messages within topic
	p.client.flushTopics()

	select {
	// signals sink that flush is complete
	case <-p.workerCtx.Done():
		return p.workerCtx.Err()
	case p.flushDone <- struct{}{}:
		return nil
	}
}

// init opens a gcp client
func (p *gcpPubsubClient) init() error {
	var client *pubsub.Client
	var err error

	creds, err := getGCPCredentials(p.ctx, p.url)
	if err != nil {
		return err
	}
	// Sending messages to the same region ensures they are received in order
	// even when multiple publishers are used.
	// region can be changed from query parameter to config option

	client, err = pubsub.NewClient(
		p.ctx,
		p.projectID,
		creds,
		option.WithEndpoint(p.endpoint),
	)

	if err != nil {
		return errors.Wrap(err, "opening client")
	}
	p.client = client
	p.mu.topics = make(map[string]*pubsub.Topic)

	return nil

}

// openTopic optimistically creates the topic
func (p *gcpPubsubClient) openTopic(topicName string) (*pubsub.Topic, error) {
	t, err := p.client.CreateTopic(p.ctx, topicName)
	if err != nil {
		switch status.Code(err) {
		case codes.AlreadyExists:
			t = p.client.Topic(topicName)
		case codes.PermissionDenied:
			// PermissionDenied may not be fatal if the topic already exists,
			// but record it in case it turns out not to.
			p.recordAutocreateErrorLocked(err)
			t = p.client.Topic(topicName)
		default:
			p.recordAutocreateErrorLocked(err)
			return nil, err
		}
	}
	t.EnableMessageOrdering = true
	return t, nil
}

func (p *gcpPubsubClient) closeTopics() {
	_ = p.forEachTopic(func(_ string, t *pubsub.Topic) error {
		t.Stop()
		return nil
	})
}

// sendMessage sends a message to the topic
func (p *gcpPubsubClient) sendMessage(m []byte, topic string, key string) error {
	t, err := p.getTopicClient(topic)
	if err != nil {
		return err
	}
	res := t.Publish(p.ctx, &pubsub.Message{
		Data:        m,
		OrderingKey: key,
	})

	// The Get method blocks until a server-generated ID or
	// an error is returned for the published message.
	_, err = res.Get(p.ctx)
	if err != nil {
		p.recordPublishErrorLocked(err)
		return err
	}

	return nil
}

func (p *gcpPubsubClient) sendMessageToAllTopics(m []byte) error {
	return p.forEachTopic(func(_ string, t *pubsub.Topic) error {
		res := t.Publish(p.ctx, &pubsub.Message{
			Data: m,
		})
		_, err := res.Get(p.ctx)
		if err != nil {
			return errors.Wrap(err, "emitting resolved timestamp")
		}
		return nil
	})
}

func (p *gcpPubsubClient) flushTopics() {
	_ = p.forEachTopic(func(_ string, t *pubsub.Topic) error {
		t.Flush()
		return nil
	})
}

func (p *gcpPubsubClient) forEachTopic(f func(name string, topicClient *pubsub.Topic) error) error {
	return p.topicNamer.Each(func(n string) error {
		t, err := p.getTopicClient(n)
		if err != nil {
			return err
		}
		return f(n, t)
	})
}

func (p *gcpPubsubClient) recordAutocreateErrorLocked(e error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.autocreateError = e
}

func (p *gcpPubsubClient) recordPublishErrorLocked(e error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.publishError = e
}

// connectivityError returns any errors encountered while writing to gcp.
func (p *gcpPubsubClient) connectivityErrorLocked() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if status.Code(p.mu.publishError) == codes.NotFound && p.mu.autocreateError != nil {
		return errors.WithHint(
			errors.Wrap(p.mu.autocreateError,
				"Topic not found, and attempt to autocreate it failed."),
			"Create topics in advance or grant this service account the pubsub.editor role on your project.")
	}
	return errors.CombineErrors(p.mu.publishError, p.mu.autocreateError)
}

// Generate the cloud endpoint that's specific to a region (e.g. us-east1).
// Ideally this would be discoverable via API but doesn't seem to be.
// A hardcoded approach looks to be correct right now.
func gcpEndpointForRegion(region string) string {
	return fmt.Sprintf("%s-pubsub.googleapis.com:443", region)
}

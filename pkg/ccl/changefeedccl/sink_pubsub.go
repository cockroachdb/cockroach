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
	"hash/crc32"
	"net/url"

	"cloud.google.com/go/pubsub"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const credentialsParam = "CREDENTIALS"
const gcpScheme = "gcppubsub"
const memScheme = "mem"
const gcpScope = "https://www.googleapis.com/auth/pubsub"
const numOfWorkers = 128

// isPubsubSInk returns true if url contains scheme with valid pubsub sink
func isPubsubSink(u *url.URL) bool {
	switch u.Scheme {
	case gcpScheme, memScheme:
		return true
	default:
		return false
	}
}

type pubsubClient interface {
	openTopics(string) error
	closeTopics(string)
	flushTopics()
	sendMessage([]byte, descpb.ID, string) error
	sendMessageToAllTopics([]byte, string) error
	getTopicName(descpb.ID) string
}

// payload struct is sent to the sink
type payload struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
	Topic string          `json:"topic"`
}

// pubsubMessage is sent to worker channels for workers to consume
type pubsubMessage struct {
	alloc   kvevent.Alloc
	message payload
	isFlush bool
	topicID descpb.ID
}

type gcpPubsubClient struct {
	client    *pubsub.Client
	creds     *google.Credentials
	topics    map[descpb.ID]*topicStruct
	ctx       context.Context
	projectID string
	region    string
}

type topicStruct struct {
	topicName   string
	topicClient *pubsub.Topic
}

type pubsubSink struct {
	url        sinkURL
	numWorkers int

	workerCtx   context.Context
	workerGroup ctxgroup.Group

	exitWorkers func()               // Signaled to shut down all workers.
	eventsChans []chan pubsubMessage //channel where messages are consumed and sent out

	// flushDone channel signaled when flushing completes.
	flushDone chan struct{}

	// errChan is written to indicate an error while sending message.
	errChan chan error

	withTopicName string

	client pubsubClient
}

// my idea is to force this to be an interface so it adheres to api, but not too sure since Sink is already an API
//func getPubsubSinkObject(url sinkURL, withTopicName bool) pubsubSink {
//	return &pubsubSinkObject{url: url, withTopicName: withTopicName}
//}

// getGCPCredentials returns gcp credentials parsed out from url
func getGCPCredentials(ctx context.Context, u sinkURL) (*google.Credentials, error) {
	const authParam = "AUTH"
	const authSpecified = "specified"
	const authImplicit = "implicit"
	const authDefault = "default"

	var credsJSON []byte
	var creds *google.Credentials
	var err error
	authOption := u.consumeParam(authParam)

	// implemented according to https://github.com/cockroachdb/cockroach/pull/64737
	switch authOption {
	case authImplicit:
		creds, err = google.FindDefaultCredentials(ctx, gcpScope)
		if err != nil {
			return nil, err
		}
		return creds, nil
	case authSpecified:
		fallthrough
	case authDefault:
		fallthrough
	default:
		err := u.decodeBase64(credentialsParam, &credsJSON)
		if err != nil {
			return nil, errors.Wrap(err, "decoding credentials json")
		}
		creds, err = google.CredentialsFromJSON(ctx, credsJSON, gcpScope)
		if err != nil {
			return nil, errors.Wrap(err, "creating credentials")
		}
		return creds, nil
	}
}

// MakePubsubSink returns the corresponding pubsub sink based on the url given
func MakePubsubSink(
	ctx context.Context, u *url.URL, opts map[string]string, targets jobspb.ChangefeedTargets,
) (Sink, error) {

	pubsubURL := sinkURL{u, u.Query()}
	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)

	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case changefeedbase.OptFormatJSON:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}

	switch changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) {
	case changefeedbase.OptEnvelopeWrapped:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, opts[changefeedbase.OptEnvelope])
	}

	//if _, ok := opts[changefeedbase.OptKeyInValue]; !ok {
	//	return nil, errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptKeyInValue)
	//}

	ctx, cancel := context.WithCancel(ctx)
	// currently just hardcoding numWorkers to 128, it will be a config option later down the road
	p := &pubsubSink{
		workerCtx: ctx, url: pubsubURL, numWorkers: numOfWorkers,
		exitWorkers: cancel, withTopicName: pubsubTopicName,
	}

	// creates custom pubsub object based on scheme
	switch u.Scheme {
	case gcpScheme:
		const regionParam = "region"
		creds, err := getGCPCredentials(ctx, p.url)
		if err != nil {
			_ = p.Close()
			return nil, err
		}
		projectID := pubsubURL.Host
		region := pubsubURL.consumeParam(regionParam)
		g := &gcpPubsubClient{creds: creds, topics: p.getTopicsMap(targets, pubsubTopicName),
			ctx: ctx, projectID: projectID, region: region}
		p.client = g
		return p, nil
	default:
		_ = p.Close()
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}
}

func (p *pubsubSink) Dial() error {
	p.setupWorkers()
	err := p.client.openTopics(p.withTopicName)
	return err
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
	m := pubsubMessage{
		alloc: alloc, isFlush: false, topicID: topic.GetID(), message: payload{
			Key:   key,
			Value: value,
			// we use getTopicName because of the option use full topic name which is not exposed in topic.GetName()
			Topic: p.client.getTopicName(topic.GetID()),
		}}

	// calculate index by hashing key
	i := p.workerIndex(key)
	select {
	// check the sink context in case workers have been terminated
	case <-p.workerCtx.Done():
		// check again for error in case it triggered since last check
		// will return more verbose error instead of "context canceled"
		return errors.CombineErrors(p.workerCtx.Err(), p.sinkError())
	case <-ctx.Done():
		return ctx.Err()
	case err := <-p.errChan:
		// check if there are any errors with sink
		return err
	case p.eventsChans[i] <- m:
		// send message to event channel
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

	return p.client.sendMessageToAllTopics(payload, p.withTopicName)
}

// Flush blocks until all messages in the event channels are sent
func (p *pubsubSink) Flush(ctx context.Context) error {
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
		return p.sinkError()
	}

}

// Close closes all the channels and shutdowns the topic
func (p *pubsubSink) Close() error {
	var err error
	p.client.closeTopics(p.withTopicName)
	p.exitWorkers()
	log.Info(p.workerCtx, "workers cancelled")
	_ = p.workerGroup.Wait()
	log.Info(p.workerCtx, "done waiting for workers")
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
	if err != nil {
		return errors.Wrap(err, "closing pubsub topic")
	}
	return nil
}

func (p *gcpPubsubClient) getTopicClient(topicID descpb.ID) (*pubsub.Topic, error) {
	if topicStruct, ok := p.topics[topicID]; ok {
		return topicStruct.topicClient, nil
	}
	return nil, errors.New("topic client does not exist")
}

func (p *pubsubSink) getTopicsMap(targets jobspb.ChangefeedTargets, pubsubTopicName string) map[descpb.ID]*topicStruct {
	topics := make(map[descpb.ID]*topicStruct)

	//creates a topic for each target
	for id, target := range targets {
		var topicName string
		if p.withTopicName != "" {
			topicName = pubsubTopicName
		} else {
			topicName = target.StatementTimeName
		}
		topics[id] = &topicStruct{topicName: topicName}
	}
	return topics
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

			m := msg.message
			b, err := json.Marshal(m)
			if err != nil {
				p.exitWorkersWithError(err)
			}
			err = p.client.sendMessage(b, msg.topicID, string(msg.message.Key))
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
func (p *pubsubSink) sinkError() error {
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

// Dial connects to gcp client and opens a topic
func (p *gcpPubsubClient) openTopics(withTopicName string) error {
	var client *pubsub.Client
	var err error

	// Sending messages to the same region ensures they are received in order
	// even when multiple publishers are used.
	// region can be changed from query parameter to config option
	if p.region != "" {
		client, err = pubsub.NewClient(p.ctx, p.projectID, option.WithCredentials(p.creds), option.WithEndpoint(p.region))
	} else {
		client, err = pubsub.NewClient(p.ctx, p.projectID, option.WithCredentials(p.creds), option.WithEndpoint(p.region))
	}

	if err != nil {
		return errors.Wrap(err, "opening client")
	}
	p.client = client

	var withTopicNameClient *pubsub.Topic
	if withTopicName != "" {
		withTopicNameClient, err = p.openTopic(withTopicName)
		if err != nil {
			return err
		}
	}

	for _, topic := range p.topics {
		if withTopicName != "" {
			topic.topicClient = withTopicNameClient
		} else {
			topic.topicClient, err = p.openTopic(topic.topicName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *gcpPubsubClient) openTopic(topicName string) (*pubsub.Topic, error) {
	t := p.client.Topic(topicName)
	topicExist, err := t.Exists(p.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "opening topic")
	}
	if !topicExist {
		t, err = p.client.CreateTopic(p.ctx, topicName)
	}
	t.EnableMessageOrdering = true
	return t, nil
}

func (p *gcpPubsubClient) closeTopics(withTopicName string) {
	for _, topicStruct := range p.topics {
		topicStruct.topicClient.Stop()
		// only need to shutdown one topic if they are all sent to the same one
		if withTopicName != "" {
			break
		}
	}
}

// sendMessage sends a message to the topic
func (p *gcpPubsubClient) sendMessage(m []byte, topicID descpb.ID, key string) error {
	t, err := p.getTopicClient(topicID)
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

	return nil
}

func (p *gcpPubsubClient) sendMessageToAllTopics(m []byte, withTopicName string) error {
	for topicID := range p.topics {
		err := p.sendMessage(m, topicID, "")
		if err != nil {
			return errors.Wrap(err, "emitting resolved timestamp")
		}

		// if with topic name option is set then you only need to send out to one of the topics
		if withTopicName != "" {
			break
		}
	}
	return nil
}

func (p *gcpPubsubClient) getTopicName(topicID descpb.ID) string {
	if topicStruct, ok := p.topics[topicID]; ok {
		return topicStruct.topicName
	}
	return ""
}

func (p *gcpPubsubClient) flushTopics() {
	for _, topicStruct := range p.topics {
		topicStruct.topicClient.Flush()
	}
}

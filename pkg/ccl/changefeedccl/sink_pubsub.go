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
	"path"

	// gcp pubsub driver used by gocloud.dev/pubsub
	_ "cloud.google.com/go/pubsub"
	gcpClient "cloud.google.com/go/pubsub/apiv1"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/gcppubsub"
	// in memory pubsub driver used by gocloud.dev/pubsub
	_ "gocloud.dev/pubsub/mempubsub"
	"golang.org/x/oauth2/google"
	pbapi "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const credentialsParam = "CREDENTIALS"
const authParam = "AUTH"
const authSpecified = "specified"
const authImplicit = "implicit"
const authDefault = "default"
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
	logIt   func()
}

type gcpPubsubSink struct {
	client     *gcpClient.PublisherClient
	cleanup    func()
	pubsubSink *pubsubSink
	creds      *google.Credentials
	conn       *grpc.ClientConn
}

type memPubsubSink struct {
	pubsubSink *pubsubSink
	isDialed   bool
}

type topicStruct struct {
	topicName   string
	pathName    string
	topicClient *pubsub.Topic
}

type pubsubSink struct {
	topics     map[descpb.ID]*topicStruct
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

	withTopicName bool
}

// getGCPCredentials returns gcp credentials parsed out from url
func getGCPCredentials(ctx context.Context, u sinkURL) (*google.Credentials, error) {
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

func createGCPURL(u sinkURL, topicName string) (string, error) {
	// TODO: look into topic name validation https://cloud.google.com/pubsub/docs/admin#resource_names
	return path.Join("projects", u.Host, "topics", topicName), nil
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
	// currently just hardcoding numWorkers to 1024, it will be a config option later down the road
	p := &pubsubSink{
		workerCtx: ctx, url: pubsubURL, numWorkers: numOfWorkers,
		exitWorkers: cancel, topics: make(map[descpb.ID]*topicStruct),
	}
	//creates a topic for each target
	for id, target := range targets {
		var topicName string
		if pubsubTopicName == "" {
			topicName = target.StatementTimeName
		} else {
			topicName = pubsubTopicName
		}
		p.topics[id] = &topicStruct{topicName: topicName}
	}
	p.setupWorkers()

	// set flag true if with topic name option is on
	p.withTopicName = pubsubTopicName == ""

	// creates custom pubsub object based on scheme
	switch u.Scheme {
	case gcpScheme:
		creds, err := getGCPCredentials(ctx, p.url)
		if err != nil {
			_ = p.close()
			return nil, err
		}
		g := &gcpPubsubSink{creds: creds, pubsubSink: p}
		return g, nil
	case memScheme:
		m := &memPubsubSink{pubsubSink: p, isDialed: false}
		return m, nil
	default:
		_ = p.close()
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}
}

// getWorkerCtx returns workerCtx
func (p *pubsubSink) getWorkerCtx() context.Context {
	return p.workerCtx
}

func timeIt(ctx context.Context, fmtOrStr string, args ...interface{}) func() {
	start := timeutil.Now()
	return func() {
		log.Infof(ctx, "%s took duration=%s",
			fmt.Sprintf(fmtOrStr, args...), timeutil.Since(start))
	}
}

// EmitRow pushes a message to event channel where it is consumed by workers
func (p *pubsubSink) emitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	m := pubsubMessage{logIt: timeIt(ctx, "key:%s, mvcc:%s", string(key), mvcc),
		alloc: alloc, isFlush: false, topicID: topic.GetID(), message: payload{
			Key:   key,
			Value: value,
			Topic: p.topics[topic.GetID()].topicName,
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
func (p *pubsubSink) emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	payload, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return errors.Wrap(err, "encoding resolved timestamp")
	}

	for topicID := range p.topics {
		err = p.sendMessage(payload, topicID, "")
		if err != nil {
			return errors.Wrap(err, "emitting resolved timestamp")
		}

		// if with topic name option is set then you only need to send out to one of the topics
		if p.withTopicName {
			break
		}
	}
	return nil
}

// Flush blocks until all messages in the event channels are sent
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
		return p.sinkError()
	}

}

// Close closes all the channels and shutdowns the topic
func (p *pubsubSink) close() error {
	var err error
	for _, topic := range p.topics {
		if topic.topicClient != nil {
			err = topic.topicClient.Shutdown(p.getWorkerCtx())
		}
	}
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

// sendMessage sends a message to the topic
func (p *pubsubSink) sendMessage(m []byte, topicID descpb.ID, key string) error {
	var c *gcpClient.PublisherClient
	var err error
	if p.topics[topicID].topicClient.As(&c) {
		gcpMessage := &pbapi.PublishRequest{Topic: p.topics[topicID].pathName}
		ts := timestamppb.Now()
		gcpMessage.Messages = append(gcpMessage.Messages, &pbapi.PubsubMessage{Data: m, OrderingKey: key, PublishTime: ts})
		_, err = c.Publish(p.workerCtx, gcpMessage)
	} else {
		// this is for mempubsub since As() on mempubsub returns an unexported topic type that we cannot interact with
		// https://github.com/google/go-cloud/blob/master/pubsub/mempubsub/mem.go#L185-L188
		err = p.topics[topicID].topicClient.Send(p.workerCtx, &pubsub.Message{
			Body: m,
		})
	}
	if err != nil {
		return err
	}
	return nil
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
			err = p.sendMessage(b, msg.topicID, string(msg.message.Key))
			if err != nil {
				p.exitWorkersWithError(err)
			}
			msg.alloc.Release(p.workerCtx)
			msg.logIt()
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
	select {
	// signals sink that flush is complete
	case <-p.workerCtx.Done():
		return p.workerCtx.Err()
	case p.flushDone <- struct{}{}:
		return nil
	}
}

// Dial connects to gcp client and opens a topic
func (p *gcpPubsubSink) Dial() error {
	// Open a gRPC connection to the GCP Pub/Sub API.
	conn, cleanup, err := gcppubsub.Dial(p.pubsubSink.getWorkerCtx(), p.creds.TokenSource)
	if err != nil {
		return errors.Wrap(err, "establishing gcp connection")
	}
	p.conn = conn

	p.cleanup = cleanup

	// Construct a PublisherClient using the connection.
	pubClient, err := gcppubsub.PublisherClient(p.pubsubSink.getWorkerCtx(), conn)
	if err != nil {
		return errors.Wrap(err, "creating publisher client")
	}
	p.client = pubClient

	for _, topic := range p.pubsubSink.topics {
		topicPath, err := createGCPURL(p.pubsubSink.url, topic.topicName)
		if err != nil {
			return errors.Wrap(err, "invalid topic name")
		}
		// TODO: implement topic config https://pkg.go.dev/cloud.google.com/go/pubsub#TopicConfig
		topic.topicClient, err = gcppubsub.OpenTopicByPath(pubClient, topicPath, nil)
		if err != nil {
			return errors.Wrap(err, "opening topic")
		}
		topic.pathName = topicPath
	}
	return nil
}

// EmitRow calls the pubsubDesc EmitRow
func (p *gcpPubsubSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	_ hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	err := p.pubsubSink.emitRow(ctx, topic, key, value, updated, alloc)
	if err != nil {
		return err
	}
	return nil
}

// EmitResolvedTimestamp calls the pubsubDesc EmitResolvedTimestamp
func (p *gcpPubsubSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	return p.pubsubSink.emitResolvedTimestamp(ctx, encoder, resolved)
}

// Flush calls the pubsubDesc Flush
func (p *gcpPubsubSink) Flush(ctx context.Context) error {
	return p.pubsubSink.flush(ctx)
}

// Close calls the pubsubDesc Close and closes the client and connection
func (p *gcpPubsubSink) Close() error {
	//	.Info(p.pubsubSink.workerCtx, "\x1b[33m closing pubsub \x1b[0m")
	if p.pubsubSink != nil {
		err := p.pubsubSink.close()
		if err != nil {
			return err
		}
	}
	if p.client != nil {
		_ = p.client.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close() // nolint:grpcconnclose
	}
	if p.cleanup != nil {
		p.cleanup()
	}
	return nil
}

// Dial returns nil
func (p *memPubsubSink) Dial() error {
	// returns nil so that canary sink can Dial and Close without erroring out
	// when the sink is reopened again with the same url
	return nil
}

// lazyDial connects mempubsub with url
func (p *memPubsubSink) lazyDial() error {
	var err error
	for _, topic := range p.pubsubSink.topics {
		topic.topicClient, err = pubsub.OpenTopic(p.pubsubSink.getWorkerCtx(), p.pubsubSink.url.String())
		if err != nil {
			return errors.Wrap(err, "opening topic")
		}
	}
	p.isDialed = true
	return nil
}

// EmitRow calls the pubsubDesc EmitRow
func (p *memPubsubSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	_ hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	if !p.isDialed {
		err := p.lazyDial()
		if err != nil {
			return errors.Wrap(err, "lazy Dial")
		}
	}
	err := p.pubsubSink.emitRow(ctx, topic, key, value, updated, alloc)
	if err != nil {
		return err
	}
	return nil
}

// EmitResolvedTimestamp calls the pubsubDesc EmitResolvedTimestamp
func (p *memPubsubSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if !p.isDialed {
		err := p.lazyDial()
		if err != nil {
			return errors.Wrap(err, "lazy Dial")
		}
	}
	return p.pubsubSink.emitResolvedTimestamp(ctx, encoder, resolved)
}

// Flush calls the pubsubDesc Flush
func (p *memPubsubSink) Flush(ctx context.Context) error {
	if !p.isDialed {
		err := p.lazyDial()
		if err != nil {
			return errors.Wrap(err, "lazy Dial")
		}
	}
	return p.pubsubSink.flush(ctx)
}

// Close calls the pubsubDesc Close
func (p *memPubsubSink) Close() error {
	err := p.pubsubSink.close()
	return err
}

package changefeedccl

import (
	"context"
	"encoding/json"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/oauth2/google"
	"hash/crc32"
	"net/url"
	"path"
	"regexp"
	"strings"

	_ "cloud.google.com/go/pubsub"
	gcpClient "cloud.google.com/go/pubsub/apiv1"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/errors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/mempubsub"
	"google.golang.org/grpc"
)

const credentialsParam = "CREDENTIALS"
const gcpScheme = "gcppubsub"
const memScheme = "mem"
const gcpScope = "https://www.googleapis.com/auth/pubsub"

var (
	fullTopicPathRE  = regexp.MustCompile("^projects/[^/]+/topics/[^/]+$")
	shortTopicPathRE = regexp.MustCompile("^[^/]+/[^/]+$")
)

//isPubsubSInk returns true if url contains scheme with valid pubsub sink
func isPubsubSink(u *url.URL) bool {
	switch u.Scheme {
	case gcpScheme, memScheme:
		return true
	default:
		return false
	}
}

//payload struct is sent to the sink
type payload struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
	Topic string `json:"topic"`
}

//pubsubMessage is sent to worker channels for workers to consume
type pubsubMessage struct {
	alloc   kvevent.Alloc
	message payload
	isFlush bool
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
}

type pubsubSink struct {
	topic      *pubsub.Topic
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
}

//getGCPCredentials returns gcp credentials parsed out from url
func getGCPCredentials(u sinkURL, ctx context.Context) (*google.Credentials, error) {
	var credsBase64 string
	var credsJSON []byte
	var q = u.q
	if credsBase64 = q.Get(credentialsParam); credsBase64 == "" {
		return nil, errors.Errorf("%s missing credentials param", q)
	}
	q.Del(credentialsParam)
	err := u.decodeBase64(credsBase64, &credsJSON)
	creds, err := google.CredentialsFromJSON(ctx, credsJSON, gcpScope)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding credentials json")
	}
	return creds, nil
}

//parseGCPURL returns fullpath of url if properly formatted
func parseGCPURL(u sinkURL) (string, error) {
	fullPath := path.Join(u.Host, u.Path)
	if fullTopicPathRE.MatchString(fullPath) {
		parts := strings.SplitN(fullPath, "/", 4)
		if len(parts) < 4 {
			return "", errors.Errorf("unexpected number of components in %s", fullPath)
		}
		return fullPath, nil
	} else if shortTopicPathRE.MatchString(fullPath) {
		return path.Join("projects", u.Host, "topics", u.Path), nil
	}
	return "", errors.Errorf("could not parse project and topic from %s", fullPath)
}

//MakePubsubSink returns the corresponding pubsub sink based on the url given
func MakePubsubSink(ctx context.Context, u *url.URL, opts map[string]string) (Sink, error) {
	log.Warningf(context.Background(), "\x1b[34m make pubsubsink \x1b[0m")
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
	//currently just harrdcoding numWorkers to 10, it will be a config option later down the road
	p := &pubsubSink{workerCtx: ctx, url: sinkURL{u, u.Query()}, numWorkers: 10, exitWorkers: cancel}
	p.setupWorkers()

	//creates custom pubsub object based on scheme
	switch u.Scheme {
	case gcpScheme:
		creds, err := getGCPCredentials(p.url, ctx)
		if err != nil {
			_ = p.close()
			return nil, err
		}
		g := &gcpPubsubSink{creds: creds, pubsubSink: p}
		return g, nil
	case memScheme:
		m := &memPubsubSink{pubsubSink: p}
		return m, nil
	default:
		_ = p.close()
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}
}

//getWorkerCtx returns workerCtx
func (p *pubsubSink) getWorkerCtx() context.Context {
	return p.workerCtx
}

//getUrl returns url
func (p *pubsubSink) getUrl() sinkURL {
	return p.url
}

//setTopic sets the topic with the argument passed in
func (p *pubsubSink) setTopic(topic *pubsub.Topic) {
	p.topic = topic
}

//EmitRow pushes a message to event channel where it is consumed by workers
func (p *pubsubSink) emitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	_ hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	log.Warningf(context.Background(), "\x1b[34m EMIT ROW \x1b[0m")
	log.Warningf(context.Background(), string(key))
	m := pubsubMessage{alloc: alloc, isFlush: false, message: payload{
		Key:   key,
		Value: value,
		Topic: topic.GetName(),
	}}
	// calculate index by hashing key
	i := p.workerIndex(key)
	select {
	// check the webhook sink context in case workers have been terminated
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

//EmitResolvedTimestamp sends resolved timestamp message
func (p *pubsubSink) emitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
	payload, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return errors.Wrapf(err, "encoding resolved timestamp")
	}
	err = p.sendMessage(payload)
	if err != nil {
		return errors.Wrapf(err, "emiting resolved timestamp")
	}
	return nil
}

//Flush blocks until all messages in the event channels are sent
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

//Close closes all the channels and shutdowns the topic
func (p *pubsubSink) close() error {
	err := p.topic.Shutdown(p.getWorkerCtx())
	if err != nil {
		return errors.Wrapf(err, "closing pubsub topic")
	}
	p.exitWorkers()
	_ = p.workerGroup.Wait()
	close(p.errChan)
	close(p.flushDone)
	for i := 0; i < p.numWorkers; i++ {
		close(p.eventsChans[i])
	}
	return nil
}

//sendMessage sends a message to the topic
func (p *pubsubSink) sendMessage(m []byte) error {
	err := p.topic.Send(p.workerCtx, &pubsub.Message{
		Body: m,
	})
	if err != nil {
		return err
	}
	return nil
}

//setupWorkers sets up the channels used by the sink and starts a goroutine for every worker
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

//workerLoop consumes any message sent to the channel corresponding to the worker index
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
			err = p.sendMessage(b)
			if err != nil {
				p.exitWorkersWithError(err)
			}
			msg.alloc.Release(p.workerCtx)
		}
	}
}

//exitWorkersWithError sends an error to the sink error channel
func (p *pubsubSink) exitWorkersWithError(err error) {
	// errChan has buffer size 1, first error will be saved to the buffer and
	// subsequent errors will be ignored
	select {
	case p.errChan <- err:
		p.exitWorkers()
	default:
	}
}

//sinkError checks if there is an error in the error channel
func (p *pubsubSink) sinkError() error {
	select {
	case err := <-p.errChan:
		return err
	default:
	}
	return nil
}

//workerIndex hashes key to return a worker index
func (p *pubsubSink) workerIndex(key []byte) uint32 {
	return crc32.ChecksumIEEE(key) % uint32(p.numWorkers)
}

//flushWorkers sends a flush message to every worker channel and then signals sink that flush is done
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
	//signals sink that flush is complete
	case <-p.workerCtx.Done():
		return p.workerCtx.Err()
	case p.flushDone <- struct{}{}:
		return nil
	}
}

//Dial connects to gcp client and opens a topic
func (p *gcpPubsubSink) Dial() error {
	// Open a gRPC connection to the GCP Pub/Sub API.
	conn, cleanup, err := gcppubsub.Dial(p.pubsubSink.getWorkerCtx(), p.creds.TokenSource)
	if err != nil {
		return errors.Wrapf(err, "establishing gcp connection")
	}
	p.conn = conn

	p.cleanup = cleanup

	// Construct a PublisherClient using the connection.
	pubClient, err := gcppubsub.PublisherClient(p.pubsubSink.getWorkerCtx(), conn)
	if err != nil {
		return errors.Wrapf(err, "creating publisher client")
	}
	p.client = pubClient

	// Construct a *pubsub.Topic.
	fullPath, err := parseGCPURL(p.pubsubSink.getUrl())
	if err != nil {
		return errors.Wrapf(err, "parsing url")
	}

	//TODO: implement topic config https://pkg.go.dev/cloud.google.com/go/pubsub#TopicConfig
	// implement odering key option for gcp https://cloud.google.com/pubsub/docs/publisher#using_ordering_keys
	topic, err := gcppubsub.OpenTopicByPath(pubClient, fullPath, nil)
	if err != nil {
		return errors.Wrapf(err, "opening topic")
	}

	p.pubsubSink.setTopic(topic)
	return nil
}

//EmitRow calls the pubsubDesc EmitRow
func (p *gcpPubsubSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	err := p.pubsubSink.emitRow(ctx, topic, key, value, updated, alloc)
	if err != nil {
		return err
	}
	return nil
}

//EmitResolvedTimestamp calls the pubsubDesc EmitResolvedTimestamp
func (p *gcpPubsubSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
	return p.pubsubSink.emitResolvedTimestamp(ctx, encoder, resolved)
}

//Flush calls the pubsubDesc Flush
func (p *gcpPubsubSink) Flush(ctx context.Context) error {
	return p.pubsubSink.flush(ctx)
}

//Close calls the pubsubDesc Close and closes the client and connection
func (p *gcpPubsubSink) Close() error {
	if p.pubsubSink != nil {
		err := p.pubsubSink.close()
		if err != nil {
			return err
		}
	}
	if p.client != nil {
		p.client.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
	if p.cleanup != nil {
		p.cleanup()
	}
	return nil
}

//Dial opens topic using url
func (p *memPubsubSink) Dial() error {
	topic, err := pubsub.OpenTopic(p.pubsubSink.getWorkerCtx(), p.pubsubSink.url.String())
	if err != nil {
		return err
	}

	p.pubsubSink.setTopic(topic)
	return nil
}

//EmitRow calls the pubsubDesc EmitRow
func (p *memPubsubSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	err := p.pubsubSink.emitRow(ctx, topic, key, value, updated, alloc)
	if err != nil {
		return err
	}
	return nil
}

//EmitResolvedTimestamp calls the pubsubDesc EmitResolvedTimestamp
func (p *memPubsubSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
	return p.pubsubSink.emitResolvedTimestamp(ctx, encoder, resolved)
}

//Flush calls the pubsubDesc Flush
func (p *memPubsubSink) Flush(ctx context.Context) error {
	return p.pubsubSink.flush(ctx)
}

//Close calls the pubsubDesc Close
func (p *memPubsubSink) Close() error {
	//if p.pubsubSink != nil {
	//	err := p.pubsubSink.close()
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

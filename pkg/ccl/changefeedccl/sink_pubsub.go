package changefeedccl

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	Key json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

//pubsubMessage is sent to worker channels for workers to consume
type pubsubMessage struct {
	alloc kvevent.Alloc
	message payload
	isFlush bool
}

type GcpPubsubSink struct {
	client *gcpClient.PublisherClient
	cleanup func()
	pubsubDesc PubsubDesc
	creds *google.Credentials
	conn *grpc.ClientConn
}

type MemPubsubSink struct {
	pubsubDesc PubsubDesc
}

type PubsubDesc interface {
	Close() error
	EmitRow(
		ctx context.Context,
		topic TopicDescriptor,
		key, value []byte,
		updated hlc.Timestamp,
		alloc kvevent.Alloc,
	) error
	EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error
	Flush(ctx context.Context) error
	getWorkerCtx() context.Context
	getUrl() *url.URL
	setTopic(*pubsub.Topic)
	setupWorkers()
}

type pubsubSink struct {
	topic  *pubsub.Topic
	url *url.URL

	numWorkers int

	workerCtx   context.Context
	workerGroup ctxgroup.Group

	exitWorkers func() // Signaled to shut down all workers.
	eventsChans []chan pubsubMessage //channel where messages are consumed and sent out

	// flushDone channel signaled when flushing completes.
	flushDone chan struct{}

	// errChan is written to indicate an error while sending message.
	errChan chan error

}

//getGCPCredentials returns gcp credentials parsed out from url
func getGCPCredentials(q *url.Values, ctx context.Context) (*google.Credentials, error){
	var credsBase64 string
	if credsBase64 = q.Get(credentialsParam); credsBase64 == "" {
		return nil, errors.Errorf("%s missing credentials param", q)
	}
	q.Del(credentialsParam)
	credsJSON, err := base64.StdEncoding.DecodeString(credsBase64)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding value of %s", credentialsParam)
	}
	creds, err := google.CredentialsFromJSON(ctx, credsJSON, gcpScope)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding credentials json")
	}
	return creds, nil
}


//parseGCPURL returns fullpath of url if properly formatted
func parseGCPURL(u *url.URL) (string, error) {
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

	if _, ok := opts[changefeedbase.OptKeyInValue]; !ok {
		return nil, errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptKeyInValue)
	}

	q := u.Query()
	ctx, cancel := context.WithCancel(ctx)
	p := &pubsubSink{workerCtx: ctx, url: u, numWorkers: 10, exitWorkers: cancel}
	p.setupWorkers()

	//creates custom pubsub object based on scheme
	switch u.Scheme {
	case gcpScheme:
		creds, err := getGCPCredentials(&q, ctx)
		if err != nil {
			_ = p.Close()
			return nil, err
		}
		g := &GcpPubsubSink{pubsubDesc: p, creds: creds}
		return g, nil
	case memScheme:
		m := &MemPubsubSink{pubsubDesc: p}
		return m, nil
	default:
		_ = p.Close()
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}
}

//getWorkerCtx returns workerCtx
func (p *pubsubSink)getWorkerCtx() context.Context{
	return p.workerCtx
}

//getUrl returns url
func (p *pubsubSink)getUrl() *url.URL{
	return p.url
}

//setTopic sets the topic with the argument passed in
func (p *pubsubSink)setTopic(topic *pubsub.Topic){
	p.topic = topic
}

//EmitRow pushes a message to event channel where it is consumed by workers
func (p *pubsubSink) 	EmitRow(
	ctx context.Context,
	_ TopicDescriptor,
	key, value []byte,
	_ hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	m := pubsubMessage{alloc: alloc, isFlush: false, message: payload{
		Key: key,
		Value: value,
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
func (p *pubsubSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
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

//Close closes all the channels and shutdowns the topic
func (p *pubsubSink) Close() error {
	_ = p.workerGroup.Wait()
	close(p.errChan)
	close(p.flushDone)
	for i := 0; i < p.numWorkers; i++ {
		close(p.eventsChans[i])
	}
	err := p.topic.Shutdown(p.getWorkerCtx())
	if err != nil {
		return errors.Wrapf(err, "closing pubsub topic")
	}
	return nil
}

//sendMessage sends a message to the topic
func (p *pubsubSink) sendMessage(m []byte) error {
	err := p.topic.Send(p.workerCtx, &pubsub.Message{
		Body: m,
	})
	if err != nil {
		return errors.Wrapf(err, "sending message")
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

			//inline func to trigger the defer alloc release when it loops
			func(){
				defer msg.alloc.Release(p.workerCtx)
				m := payload{Key: msg.message.Key, Value: msg.message.Value}
				b, err := json.Marshal(m)
				if err != nil {
					p.exitWorkersWithError(err)
				}
				err = p.sendMessage(b)
				if err != nil {
					p.exitWorkersWithError(err)
				}
			}()
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
func (p *pubsubSink) flushWorkers() error{
	for i := 0; i < p.numWorkers; i++{
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
func (p *GcpPubsubSink)Dial() error{
	// Open a gRPC connection to the GCP Pub/Sub API.
	conn, cleanup, err := gcppubsub.Dial(p.pubsubDesc.getWorkerCtx(), p.creds.TokenSource)
	if err != nil {
		return errors.Wrapf(err, "establishing gcp connection")
	}
	p.conn = conn

	p.cleanup = cleanup

	// Construct a PublisherClient using the connection.
	pubClient, err := gcppubsub.PublisherClient(p.pubsubDesc.getWorkerCtx(), conn)
	if err != nil {
		return errors.Wrapf(err, "creating publisher client")
	}
	p.client = pubClient

	// Construct a *pubsub.Topic.
	fullPath, err := parseGCPURL(p.pubsubDesc.getUrl())
	if err != nil {
		return errors.Wrapf(err, "parsing url")
	}

	//TODO: implement options
	topic, err := gcppubsub.OpenTopicByPath(pubClient, fullPath, nil)
	if err != nil {
		return errors.Wrapf(err, "opening topic")
	}

	p.pubsubDesc.setTopic(topic)
	return nil
}

//EmitRow calls the pubsubDesc EmitRow
func (p *GcpPubsubSink) 	EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	err := p.pubsubDesc.EmitRow(ctx, topic, key, value, updated, alloc)
	if err != nil {
		return errors.Wrapf(err, "emitting row")
	}
	return nil
}

//EmitResolvedTimestamp calls the pubsubDesc EmitResolvedTimestamp
func (p *GcpPubsubSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
	return p.pubsubDesc.EmitResolvedTimestamp(ctx, encoder, resolved)
}

//Flush calls the pubsubDesc Flush
func (p *GcpPubsubSink) Flush(ctx context.Context) error {
	return p.pubsubDesc.Flush(ctx)
}

//Close calls the pubsubDesc Close and closes the client and connection
func (p *GcpPubsubSink) Close() error {
	if p.pubsubDesc != nil {
		err := p.pubsubDesc.Close()
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
func (p *MemPubsubSink)Dial() error{
	topic, err := pubsub.OpenTopic(p.pubsubDesc.getWorkerCtx(), p.pubsubDesc.getUrl().String())
	if err != nil {
		return err
	}

	p.pubsubDesc.setTopic(topic)
	return nil
}

//EmitRow calls the pubsubDesc EmitRow
func (p *MemPubsubSink) 	EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	err := p.pubsubDesc.EmitRow(ctx, topic, key, value, updated, alloc)
	if err != nil {
		return errors.Wrapf(err, "emitting row")
	}
	return nil
}

//EmitResolvedTimestamp calls the pubsubDesc EmitResolvedTimestamp
func (p *MemPubsubSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
	return p.EmitResolvedTimestamp(ctx, encoder, resolved)
}

//Flush calls the pubsubDesc Flush
func (p *MemPubsubSink) Flush(ctx context.Context) error {
	return p.pubsubDesc.Flush(ctx)
}

//Close calls the pubsubDesc Close
func (p *MemPubsubSink) Close() error {
	if p.pubsubDesc != nil {
		err := p.pubsubDesc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}





package changefeedccl

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"golang.org/x/oauth2/google"
	"path"
	"regexp"
	"strings"

	//"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"net/url"

	_ "cloud.google.com/go/pubsub"
	gcpClient "cloud.google.com/go/pubsub/apiv1"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	//"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/mempubsub"
	"google.golang.org/grpc"
)

const credentialsParam = "CREDENTIALS"
const gcpScheme = "gcppubsub"
const gcpScope = "https://www.googleapis.com/auth/pubsub"

var (
	fullTopicPathRE  = regexp.MustCompile("^projects/[^/]+/topics/[^/]+$")
	shortTopicPathRE = regexp.MustCompile("^[^/]+/[^/]+$")
)

func isPubsubSink(u *url.URL) bool {
	return u.Scheme == "gcppubsub" || u.Scheme == "mem"
}

type pubsubMessage struct {
	key json.RawMessage
	value json.RawMessage
}

type GcpPubsubSink struct {
	client *gcpClient.PublisherClient
	cleanup func()
	pubsubDesc PubsubDesc
	creds *google.Credentials
	conn *grpc.ClientConn
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
	getCtx() *context.Context
	getUrl() *url.URL
	setTopic(*pubsub.Topic)
}

type pubsubSink struct {
	topic  *pubsub.Topic
	ctx context.Context
	url *url.URL
}

func sendMessage(topic  *pubsub.Topic, ctx *context.Context, m []byte) error {
	err := topic.Send(*ctx, &pubsub.Message{
		Body: m,
	})
	return err
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

func MakePubsubSink(u *url.URL, opts map[string]string) (Sink, error) {

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

	ctx := context.TODO()

	q := u.Query()

	switch u.Scheme {
	case gcpScheme:
		var credsBase64 string
		if credsBase64 = q.Get(credentialsParam); credsBase64 == "" {
			return nil, errors.Errorf("%s missing credentials param", u.String())
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

		p := &pubsubSink{ctx: ctx, url: u}
		g := &GcpPubsubSink{pubsubDesc: p, creds: creds}
		return g, nil
	default:
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}
}

func (p *pubsubSink)getCtx() *context.Context{
	return &p.ctx
}

func (p *pubsubSink)getUrl() *url.URL{
	return p.url
}

func (p *pubsubSink)setTopic(topic *pubsub.Topic){
	p.topic = topic
}

func (p *pubsubSink) 	EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	m := pubsubMessage{key: key, value: value}
	b, err := json.Marshal(m)
	err = sendMessage(p.topic, &p.ctx, b)
	if err != nil {
		return err
	}
	alloc.Release(ctx)
	return nil
}

func (p *pubsubSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
	payload, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	err = sendMessage(p.topic, &p.ctx, payload)
	return err
}

func (p *pubsubSink) Flush(ctx context.Context) error {
	return nil
}

func (p *pubsubSink) Close() error {
	err := p.topic.Shutdown(*p.getCtx())
	if err != nil {
		return errors.Wrapf(err, "closing pubsub topic")
	}
	return nil
}

func (p *GcpPubsubSink)Dial() error{
	// Open a gRPC connection to the GCP Pub/Sub API.
	conn, cleanup, err := gcppubsub.Dial(*p.pubsubDesc.getCtx(), p.creds.TokenSource)
	if err != nil {
		return errors.Wrapf(err, "establishing gcp connection")
	}
	p.conn = conn

	p.cleanup = cleanup

	// Construct a PublisherClient using the connection.
	pubClient, err := gcppubsub.PublisherClient(*p.pubsubDesc.getCtx(), conn)
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

func (p *GcpPubsubSink) 	EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	defer alloc.Release(ctx)
	err := p.EmitRow(ctx, topic, key, value, updated, alloc)
	if err != nil {
		return errors.Wrapf(err, "emitting row")
	}
	return nil
}

func (p *GcpPubsubSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
	return p.EmitResolvedTimestamp(ctx, encoder, resolved)
}

func (p *GcpPubsubSink) Flush(ctx context.Context) error {
	return nil
}

func (p *GcpPubsubSink) Close() error {
	if p.pubsubDesc != nil {
		err := p.pubsubDesc.Close()
		if err != nil {
			return err
		}
	}
	if p.client != nil {
		p.client.Close()
		//if err != nil {
		//	return errors.Wrapf(err, "closing gcp client")
		//}
	}
	if p.conn != nil {
		p.conn.Close()
		//if err != nil {
		//	return errors.Wrapf(err, "closing gcp connection")
		//}
	}
	if p.cleanup != nil {
		p.cleanup()
	}
	return nil
}



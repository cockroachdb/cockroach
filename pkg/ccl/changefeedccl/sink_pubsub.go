package changefeedccl

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/mempubsub"
)

func isPubsubSink(u *url.URL) bool {
	return u.Scheme == "gcppubsub" || u.Scheme == "mem"
}

type pubsubSink struct {
	topic *pubsub.Topic
}

func makePubsubSink(baseURI string, opts map[string]string) (Sink, error) {

	switch formatType(opts[optFormat]) {
	case optFormatJSON:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			optFormat, opts[optFormat])
	}

	switch envelopeType(opts[optEnvelope]) {
	case optEnvelopeWrapped:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			optEnvelope, opts[optEnvelope])
	}

	if _, ok := opts[optKeyInValue]; !ok {
		return nil, errors.Errorf(`this sink requires the WITH %s option`, optKeyInValue)
	}

	ctx := context.TODO()
	var err error
	topic, err := pubsub.OpenTopic(ctx, baseURI)
	if err != nil {
		return nil, err
	}
	return &pubsubSink{
		topic: topic,
	}, nil
}

func (p *pubsubSink) EmitRow(
	ctx context.Context, table *sqlbase.TableDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	msg := pubsub.Message{
		Metadata: map[string]string{
			"topic": table.Name,
		},
		Body: value,
	}
	return errors.Wrap(p.topic.Send(ctx, &msg), "failed to send")
}

func (p *pubsubSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(ctx, noTopic, resolved)
	if err != nil {
		return err
	}
	msg := pubsub.Message{
		Body: payload,
	}
	return errors.Wrap(p.topic.Send(ctx, &msg), "failed to emit resolved timestamp")
}

func (p *pubsubSink) Flush(ctx context.Context) error {
	return nil
}

func (p *pubsubSink) Close() error {
	//	return p.topic.Shutdown(context.TODO())
	return nil
}

var _ Sink = (*pubsubSink)(nil)

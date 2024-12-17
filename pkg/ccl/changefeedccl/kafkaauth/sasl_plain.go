package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosaslplain "github.com/twmb/franz-go/pkg/sasl/plain"
)

type saslPlainBuilder struct{}

// name implements authMechanismBuilder.
func (s saslPlainBuilder) name() string {
	return sarama.SASLTypePlaintext
}

// validateParams implements AuthMechanism.
func (s saslPlainBuilder) validateParams(u *changefeedbase.SinkURL) error {
	requiredParams := []string{SASLUser, SASLPassword}
	return peekValidateParams(sarama.SASLTypePlaintext, u, requiredParams, nil)
}

// build implements authMechanismBuilder.
func (s saslPlainBuilder) build(u *changefeedbase.SinkURL) (saslMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}
	return &saslPlain{
		user:      u.ConsumeParam(SASLUser),
		password:  u.ConsumeParam(SASLPassword),
		handshake: handshake,
	}, nil
}

var _ saslMechanismBuilder = saslPlainBuilder{}

type saslPlain struct {
	user      string
	password  string
	handshake bool
}

// ApplySarama implements AuthMechanism.
func (s *saslPlain) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.Handshake = s.handshake
	cfg.Net.SASL.User = s.user
	cfg.Net.SASL.Password = s.password
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *saslPlain) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	mech := kgosaslplain.Plain(func(ctc context.Context) (kgosaslplain.Auth, error) {
		return kgosaslplain.Auth{
			User: s.user,
			Pass: s.password,
		}, nil
	})

	return []kgo.Opt{
		kgo.SASL(mech),
	}, nil
}

var _ saslMechanism = (*saslPlain)(nil)

func init() {
	Registry.Register(saslPlainBuilder{})
}

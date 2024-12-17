package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosaslplain "github.com/twmb/franz-go/pkg/sasl/plain"
)

type saslPlainBuilder struct{}

// matches implements authMechanismBuilder.
func (s saslPlainBuilder) matches(params queryParams) bool {
	return params.peek(SASLEnabled) == "true" && params.peek(SASLMechanism) == sarama.SASLTypePlaintext
}

// validateParams implements AuthMechanism.
func (s saslPlainBuilder) validateParams(params queryParams) error {
	requiredParams := []string{SASLUser, SASLPassword}
	return peekValidateParams(sarama.SASLTypePlaintext, params, requiredParams, nil)
}

// build implements authMechanismBuilder.
func (s saslPlainBuilder) build(params queryParams) (AuthMechanism, error) {
	_ = params.consume(SASLEnabled)
	_ = params.consume(SASLMechanism)
	handshake := params.consume(SASLHandshake)
	return &saslPlain{
		user:      params.consume(SASLUser),
		password:  params.consume(SASLPassword),
		handshake: handshake == "" || handshake == "true",
	}, nil
}

var _ authMechanismBuilder = saslPlainBuilder{}

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

var _ AuthMechanism = (*saslPlain)(nil)

func init() {
	Registry.Register(saslPlainBuilder{})
}

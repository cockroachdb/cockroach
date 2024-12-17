package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	kgosaslscram "github.com/twmb/franz-go/pkg/sasl/scram"
)

type saslSCRAMSHA256Builder struct{}

// name implements authMechanismBuilder.
func (s saslSCRAMSHA256Builder) name() string {
	return sarama.SASLTypeSCRAMSHA256
}

// validateParams implements authMechanismBuilder.
func (s saslSCRAMSHA256Builder) validateParams(params queryParams) error {
	requiredParams := []string{SASLUser, SASLPassword}
	return peekValidateParams(s.name(), params, requiredParams, nil)
}

// build implements authMechanismBuilder.
func (s saslSCRAMSHA256Builder) build(params queryParams) (saslMechanism, error) {
	_ = params.consume(SASLEnabled)
	_ = params.consume(SASLMechanism)
	handshake := params.consume(SASLHandshake)
	return &saslSCRAMSHA{
		user:      params.consume(SASLUser),
		password:  params.consume(SASLPassword),
		handshake: handshake == "" || handshake == "true",
		depth:     sha256,
	}, nil
}

var _ saslMechanismBuilder = saslSCRAMSHA256Builder{}

type saslSCRAMSHA512Builder struct{}

// name implements authMechanismBuilder.
func (s saslSCRAMSHA512Builder) name() string {
	return sarama.SASLTypeSCRAMSHA512
}

// validateParams implements authMechanismBuilder.
func (s saslSCRAMSHA512Builder) validateParams(params queryParams) error {
	requiredParams := []string{SASLUser, SASLPassword}
	return peekValidateParams(s.name(), params, requiredParams, nil)
}

// build implements authMechanismBuilder.
func (s saslSCRAMSHA512Builder) build(params queryParams) (saslMechanism, error) {
	_ = params.consume(SASLEnabled)
	_ = params.consume(SASLMechanism)
	handshake := params.consume(SASLHandshake)
	return &saslSCRAMSHA{
		user:      params.consume(SASLUser),
		password:  params.consume(SASLPassword),
		handshake: handshake == "" || handshake == "true",
		depth:     sha512,
	}, nil
}

var _ saslMechanismBuilder = saslSCRAMSHA512Builder{}

type shaDepth int

const (
	sha256 shaDepth = 256
	sha512 shaDepth = 512
)

type saslSCRAMSHA struct {
	depth     shaDepth
	user      string
	password  string
	handshake bool
}

// ApplySarama implements AuthMechanism.
func (s *saslSCRAMSHA) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = s.handshake
	cfg.Net.SASL.User = s.user
	cfg.Net.SASL.Password = s.password
	// TODO: better
	switch s.depth {
	case sha256:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = nil // TODO: sha256ClientGenerator
	case sha512:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = nil // TODO: sha512ClientGenerator
	default:
		return errors.AssertionFailedf("unknown SCRAM SHA depth %d", s.depth)
	}
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *saslSCRAMSHA) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	var fn func(func(ctx context.Context) (kgosaslscram.Auth, error)) sasl.Mechanism
	switch s.depth {
	case sha256:
		fn = kgosaslscram.Sha256
	case sha512:
		fn = kgosaslscram.Sha512
	default:
		return nil, errors.AssertionFailedf("unknown SCRAM SHA depth %d", s.depth)
	}

	mech := fn(func(ctc context.Context) (kgosaslscram.Auth, error) {
		return kgosaslscram.Auth{
			User: s.user,
			Pass: s.password,
		}, nil
	})

	return []kgo.Opt{
		kgo.SASL(mech),
	}, nil
}

var _ saslMechanism = (*saslSCRAMSHA)(nil)

func init() {
	Registry.Register(saslSCRAMSHA256Builder{})
	Registry.Register(saslSCRAMSHA512Builder{})
}

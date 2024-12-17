package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	kgosaslscram "github.com/twmb/franz-go/pkg/sasl/scram"
)

// SCRAMSHA256, SCRAMSHA512
// 		config.Net.SASL.SCRAMClientGeneratorFunc = sha512ClientGenerator

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

// PickMe implements AuthMechanism.
func (s *saslSCRAMSHA) PickMe(params queryParams) (AuthMechanism, bool) {
	if params.get(SASLEnabled) == "true" &&
		(params.get(SASLMechanism) == sarama.SASLTypeSCRAMSHA256 || params.get(SASLMechanism) == sarama.SASLTypeSCRAMSHA512) {
		return &saslSCRAMSHA{
			user:      params.get(SASLUser),
			password:  params.get(SASLPassword),
			handshake: params.get(SASLHandshake) == "" || params.get(SASLHandshake) == "true",
		}, true
	}
	return nil, false
}

// ValidateParams implements AuthMechanism.
func (s *saslSCRAMSHA) ValidateParams(params queryParams) error {
	requiredParams := []string{SASLUser, SASLPassword}
	forbiddenParams := oauthOnlyParams
	return validateParams(s.Name(), params, requiredParams, forbiddenParams)
}

// ApplySarama implements AuthMechanism.
func (s *saslSCRAMSHA) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = s.handshake
	cfg.Net.SASL.User = s.user
	cfg.Net.SASL.Password = s.password
	switch s.depth {
	case sha256:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case sha512:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	default:
		return errors.AssertionFailedf("unknown SCRAM depth %d", s.depth)
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
		return nil, errors.AssertionFailedf("unknown SCRAM depth %d", s.depth)
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

// Name implements AuthMechanism.
func (s *saslSCRAMSHA) Name() AuthMechanismName {
	switch s.depth {
	case sha256:
		return sarama.SASLTypeSCRAMSHA256
	case sha512:
		return sarama.SASLTypeSCRAMSHA512
	default:
		return "unknown"
	}
}

var _ AuthMechanism = (*saslSCRAMSHA)(nil)

func init() {
	Registry.Register((&saslSCRAMSHA{}).PickMe)
}

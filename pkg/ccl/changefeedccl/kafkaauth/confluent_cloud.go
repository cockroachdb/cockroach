package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kgo"
)

type confluentCloud struct {
	user      string
	password  string
	handshake bool
}

// PickMe implements AuthMechanism.
func (s *confluentCloud) PickMe(params queryParams) (AuthMechanism, bool) {
	if params.get(SASLEnabled) == "true" && params.get(SASLMechanism) == sarama.SASLTypePlaintext {
		return &confluentCloud{
			user:      params.get(SASLUser),
			password:  params.get(SASLPassword),
			handshake: params.get(SASLHandshake) == "" || params.get(SASLHandshake) == "true",
		}, true
	}
	return nil, false
}

// ValidateParams implements AuthMechanism.
func (s *confluentCloud) ValidateParams(params queryParams) error {
	requiredParams := []string{SASLUser, SASLPassword}
	forbiddenParams := oauthOnlyParams
	return validateParams(s.Name(), params, requiredParams, forbiddenParams)
}

// ApplySarama implements AuthMechanism.
func (s *confluentCloud) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.Handshake = s.handshake
	cfg.Net.SASL.User = s.user
	cfg.Net.SASL.Password = s.password
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *confluentCloud) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	mech := kgoconfluentCloud.Plain(func(ctc context.Context) (kgoconfluentCloud.Auth, error) {
		return kgoconfluentCloud.Auth{
			User: s.user,
			Pass: s.password,
		}, nil
	})

	return []kgo.Opt{
		kgo.SASL(mech),
	}, nil
}

// Name implements AuthMechanism.
func (s *confluentCloud) Name() AuthMechanismName {
	return "SASL_PLAIN"
}

var _ AuthMechanism = (*confluentCloud)(nil)

func init() {
	Registry.Register((&confluentCloud{}).PickMe)
}

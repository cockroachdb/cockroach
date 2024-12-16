package kafkaauth

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosaslplain "github.com/twmb/franz-go/pkg/sasl/plain"
)

type SASLPlain struct {
	user      string
	password  string
	handshake bool
}

// PickMe implements AuthMechanism.
func (s *SASLPlain) PickMe(params queryParams) (AuthMechanism, bool) {
	if params.get(SASLEnabled) == "true" && params.get(SASLMechanism) == "PLAIN" {
		return &SASLPlain{
			user:      params.get(SASLUser),
			password:  params.get(SASLPassword),
			handshake: params.get(SASLHandshake) == "" || params.get(SASLHandshake) == "true",
		}, true
	}
	return nil, false
}

// RequiredParams implements AuthMechanism.
func (s *SASLPlain) RequiredParams() []string {
	return []string{SASLUser, SASLPassword}
}

// ForbiddenParams implements AuthMechanism.
func (s *SASLPlain) ForbiddenParams() []string {
	return oauthOnlyParams
}

// ValidateParams implements AuthMechanism.
func (s *SASLPlain) ValidateParams(params queryParams) error {
	var errs []error
	if params.get(SASLUser) == "" {
		errs = append(errs, newRequiredParamError(SASLUser))
	}
	if params.get(SASLPassword) == "" {
		errs = append(errs, newRequiredParamError(SASLPassword))
	}
	return errors.Join(errs...)
}

// ApplySarama implements AuthMechanism.
func (s *SASLPlain) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = s.handshake
	cfg.Net.SASL.User = s.user
	cfg.Net.SASL.Password = s.password
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *SASLPlain) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
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

// Name implements AuthMechanism.
func (s *SASLPlain) Name() AuthMechanismName {
	return "SASL_PLAIN"
}

var _ AuthMechanism = (*SASLPlain)(nil)

func init() {
	Registry.Register((&SASLPlain{}).PickMe)
}

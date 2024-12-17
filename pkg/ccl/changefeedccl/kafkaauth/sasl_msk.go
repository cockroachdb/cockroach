package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosasloauth "github.com/twmb/franz-go/pkg/sasl/oauth"
)

type saslMSK struct {
	region, roleArn, sessionName string
	handshake                    bool
}

// PickMe implements AuthMechanism.
func (s *saslMSK) PickMe(params queryParams) (AuthMechanism, bool) {
	if params.get(SASLEnabled) == "true" && params.get(SASLMechanism) == "AWS_MSK_IAM" {
		return &saslMSK{
			region:      params.get(SASLAWSRegion),
			roleArn:     params.get(SASLAWSIAMRoleArn),
			sessionName: params.get(SASLAWSIAMSessionName),
			handshake:   params.get(SASLHandshake) == "" || params.get(SASLHandshake) == "true",
		}, true
	}
	return nil, false
}

// ValidateParams implements AuthMechanism.
func (s *saslMSK) ValidateParams(params queryParams) error {
	requiredParams := []string{SASLAWSRegion, SASLAWSIAMRoleArn, SASLAWSIAMSessionName}
	return validateParams(s.Name(), params, requiredParams, oauthOnlyParams)
}

// ApplySarama implements AuthMechanism.
func (s *saslMSK) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	tp, err := s.newSaramaTokenProvider(ctx)
	if err != nil {
		return err
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = s.handshake
	cfg.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	cfg.Net.SASL.TokenProvider = tp
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *saslMSK) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	tp, err := s.newKgoTokenProvider(ctx)
	if err != nil {
		return nil, err
	}

	return []kgo.Opt{kgo.SASL(kgosasloauth.Oauth(tp))}, nil
}

// Name implements AuthMechanism.
func (s *saslMSK) Name() AuthMechanismName {
	return "SASL_OAUTHBEARER"
}

func (s *saslMSK) newSaramaTokenProvider(ctx context.Context) (sarama.AccessTokenProvider, error) {
	return &saramaAWSIAMTokenProvider{
		ctx:            ctx,
		awsRegion:      s.region,
		iamRoleArn:     s.roleArn,
		iamSessionName: s.sessionName,
	}, nil
}

func (s *saslMSK) newKgoTokenProvider(_ context.Context) (func(ctx context.Context) (kgosasloauth.Auth, error), error) {
	return func(ctx context.Context) (kgosasloauth.Auth, error) {
		token, _, err := signer.GenerateAuthTokenFromRole(
			ctx, s.region, s.roleArn, s.sessionName)
		if err != nil {
			return kgosasloauth.Auth{}, err
		}
		return kgosasloauth.Auth{Token: token}, nil
	}, nil
}

var _ AuthMechanism = (*saslMSK)(nil)

type saramaAWSIAMTokenProvider struct {
	ctx            context.Context
	awsRegion      string
	iamRoleArn     string
	iamSessionName string
}

func (p *saramaAWSIAMTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthTokenFromRole(
		p.ctx, p.awsRegion, p.iamRoleArn, p.iamSessionName)
	if err != nil {
		return nil, err
	}

	return &sarama.AccessToken{Token: token}, nil
}

var _ sarama.AccessTokenProvider = (*saramaOauthTokenProvider)(nil)

func init() {
	Registry.Register((&saslMSK{}).PickMe)
}

package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosasloauth "github.com/twmb/franz-go/pkg/sasl/oauth"
)

type saslMSKBuilder struct{}

// name implements authMechanismBuilder.
func (s saslMSKBuilder) name() string {
	return "AWS_MSK_IAM"
}

// validateParams implements authMechanismBuilder.
func (s saslMSKBuilder) validateParams(params queryParams) error {
	requiredParams := []string{SASLAWSRegion, SASLAWSIAMRoleArn, SASLAWSIAMSessionName}
	return peekValidateParams(sarama.SASLTypeOAuth, params, requiredParams, nil)
}

// build implements authMechanismBuilder.
func (s saslMSKBuilder) build(params queryParams) (saslMechanism, error) {
	_ = params.consume(SASLEnabled)
	_ = params.consume(SASLMechanism)
	handshake := params.consume(SASLHandshake)
	return &saslMSK{
		region:      params.consume(SASLAWSRegion),
		roleArn:     params.consume(SASLAWSIAMRoleArn),
		sessionName: params.consume(SASLAWSIAMSessionName),
		handshake:   handshake == "" || handshake == "true",
	}, nil
}

var _ saslMechanismBuilder = saslMSKBuilder{}

type saslMSK struct {
	region, roleArn, sessionName string
	handshake                    bool
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

var _ saslMechanism = (*saslMSK)(nil)

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
	Registry.Register(saslMSKBuilder{})
}

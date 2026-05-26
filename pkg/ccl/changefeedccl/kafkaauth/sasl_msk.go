// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosasloauth "github.com/twmb/franz-go/pkg/sasl/oauth"
)

type saslMSKBuilder struct{}

// name implements authMechanismBuilder.
func (s saslMSKBuilder) name() string {
	return "AWS_MSK_IAM"
}

// validateParams implements authMechanismBuilder.
func (s saslMSKBuilder) validateParams(u *changefeedbase.SinkURL) error {
	requiredParams := []string{changefeedbase.SinkParamSASLAwsRegion, changefeedbase.SinkParamSASLAwsIAMRoleArn, changefeedbase.SinkParamSASLAwsIAMSessionName}
	return peekAndRequireParams(s.name(), u, requiredParams)
}

// build implements authMechanismBuilder.
func (s saslMSKBuilder) build(u *changefeedbase.SinkURL) (SASLMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}
	return &saslMSK{
		region:      u.ConsumeParam(changefeedbase.SinkParamSASLAwsRegion),
		roleArn:     u.ConsumeParam(changefeedbase.SinkParamSASLAwsIAMRoleArn),
		sessionName: u.ConsumeParam(changefeedbase.SinkParamSASLAwsIAMSessionName),
		handshake:   handshake,
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
	applySaramaCommon(cfg, sarama.SASLTypeOAuth, s.handshake)
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

func (s *saslMSK) newKgoTokenProvider(
	_ context.Context,
) (func(ctx context.Context) (kgosasloauth.Auth, error), error) {
	return func(ctx context.Context) (kgosasloauth.Auth, error) {
		token, _, err := signer.GenerateAuthTokenFromRole(
			ctx, s.region, s.roleArn, s.sessionName)
		if err != nil {
			return kgosasloauth.Auth{}, err
		}
		return kgosasloauth.Auth{Token: token}, nil
	}, nil
}

var _ SASLMechanism = (*saslMSK)(nil)

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
	registry.register(saslMSKBuilder{})
}

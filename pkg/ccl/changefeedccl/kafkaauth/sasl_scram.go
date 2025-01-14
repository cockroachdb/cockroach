// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
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
func (s saslSCRAMSHA256Builder) validateParams(u *changefeedbase.SinkURL) error {
	requiredParams := []string{changefeedbase.SinkParamSASLUser, changefeedbase.SinkParamSASLPassword}
	return peekAndRequireParams(s.name(), u, requiredParams)
}

// build implements authMechanismBuilder.
func (s saslSCRAMSHA256Builder) build(u *changefeedbase.SinkURL) (SASLMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}
	return &saslSCRAMSHA{
		user:      u.ConsumeParam(changefeedbase.SinkParamSASLUser),
		password:  u.ConsumeParam(changefeedbase.SinkParamSASLPassword),
		handshake: handshake,
		depth:     shaDepth256,
	}, nil
}

var _ saslMechanismBuilder = saslSCRAMSHA256Builder{}

type saslSCRAMSHA512Builder struct{}

// name implements authMechanismBuilder.
func (s saslSCRAMSHA512Builder) name() string {
	return sarama.SASLTypeSCRAMSHA512
}

// validateParams implements authMechanismBuilder.
func (s saslSCRAMSHA512Builder) validateParams(u *changefeedbase.SinkURL) error {
	requiredParams := []string{changefeedbase.SinkParamSASLUser, changefeedbase.SinkParamSASLPassword}
	return peekAndRequireParams(s.name(), u, requiredParams)
}

// build implements authMechanismBuilder.
func (s saslSCRAMSHA512Builder) build(u *changefeedbase.SinkURL) (SASLMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}
	return &saslSCRAMSHA{
		user:      u.ConsumeParam(changefeedbase.SinkParamSASLUser),
		password:  u.ConsumeParam(changefeedbase.SinkParamSASLPassword),
		handshake: handshake,
		depth:     shaDepth512,
	}, nil
}

var _ saslMechanismBuilder = saslSCRAMSHA512Builder{}

type shaDepth int

const (
	shaDepth256 shaDepth = 256
	shaDepth512 shaDepth = 512
)

type saslSCRAMSHA struct {
	depth     shaDepth
	user      string
	password  string
	handshake bool
}

// ApplySarama implements AuthMechanism.
func (s *saslSCRAMSHA) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	var mechName sarama.SASLMechanism
	switch s.depth {
	case shaDepth256:
		mechName = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = sha256ClientGenerator
	case shaDepth512:
		mechName = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = sha512ClientGenerator
	default:
		return errors.AssertionFailedf("unknown SCRAM SHA depth %d", s.depth)
	}
	applySaramaCommon(cfg, mechName, s.handshake)
	cfg.Net.SASL.User = s.user
	cfg.Net.SASL.Password = s.password
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *saslSCRAMSHA) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	var mechFn func(func(ctx context.Context) (kgosaslscram.Auth, error)) sasl.Mechanism
	switch s.depth {
	case shaDepth256:
		mechFn = kgosaslscram.Sha256
	case shaDepth512:
		mechFn = kgosaslscram.Sha512
	default:
		return nil, errors.AssertionFailedf("unknown SCRAM SHA depth %d", s.depth)
	}

	mech := mechFn(func(ctc context.Context) (kgosaslscram.Auth, error) {
		return kgosaslscram.Auth{
			User: s.user,
			Pass: s.password,
		}, nil
	})

	return []kgo.Opt{
		kgo.SASL(mech),
	}, nil
}

var _ SASLMechanism = (*saslSCRAMSHA)(nil)

func init() {
	registry.register(saslSCRAMSHA256Builder{})
	registry.register(saslSCRAMSHA512Builder{})
}

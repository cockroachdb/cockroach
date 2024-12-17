package kafkaauth

import (
	"context"
	"runtime"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type saslMechanismBuilder interface {
	name() string
	validateParams(u *changefeedbase.SinkURL) error
	build(u *changefeedbase.SinkURL) (saslMechanism, error)
}

type saslMechanism interface {
	ApplySarama(ctx context.Context, cfg *sarama.Config) error
	KgoOpts(ctx context.Context) ([]kgo.Opt, error)
}

type saslMechanismRegistry map[string]saslMechanismBuilder

var Registry saslMechanismRegistry = make(map[string]saslMechanismBuilder)

func (r saslMechanismRegistry) Register(b saslMechanismBuilder) {
	r[b.name()] = b
}

func (r saslMechanismRegistry) Pick(u *changefeedbase.SinkURL) (saslMechanism, bool, error) {
	if u == nil {
		runtime.Breakpoint()
		panic("why is there a nil url?")
	}

	var enabled bool
	if _, err := u.ConsumeBool(SASLEnabled, &enabled); err != nil {
		return nil, false, err
	}
	if !enabled {
		return nil, false, nil
	}

	b, ok := r[u.ConsumeParam(SASLMechanism)]
	if !ok {
		return nil, false, nil
	}
	if err := b.validateParams(u); err != nil {
		return nil, false, err
	}
	mech, err := b.build(u)
	if err != nil {
		return nil, false, err
	}
	return mech, true, nil
}

/// helpers

func newRequiredParamError(mechName string, param string) error {
	return errors.Newf("%s must be provided when SASL is enabled using mechanism %s", param, mechName)
}

func newForbiddenParamError(mechName string, param string) error {
	return errors.Newf("forbidden parameter %s provided for %s", param, mechName)
}

// TODO: embedded common struct? or nah
func peekValidateParams(mechName string, u *changefeedbase.SinkURL, requiredParams, forbiddenParams []string) error {
	var errs []error
	for _, param := range requiredParams {
		if u.PeekParam(param) == "" {
			errs = append(errs, newRequiredParamError(mechName, param))
		}
	}
	for _, param := range forbiddenParams {
		if u.PeekParam(param) != "" {
			errs = append(errs, newForbiddenParamError(mechName, param))
		}
	}
	return errors.Join(errs...)
}

func consumeHandshake(u *changefeedbase.SinkURL) (bool, error) {
	var handshake bool
	set, err := u.ConsumeBool(SASLHandshake, &handshake)
	if err != nil {
		return false, err
	}
	if !set {
		handshake = true
	}
	return handshake, nil
}

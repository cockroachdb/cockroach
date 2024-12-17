package kafkaauth

import (
	"context"
	"net/url"
	"runtime"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type saslMechanismBuilder interface {
	name() string
	validateParams(params queryParams) error
	build(params queryParams) (saslMechanism, error)
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

func (r saslMechanismRegistry) Pick(u *url.URL) (saslMechanism, bool, error) {
	if u == nil {
		runtime.Breakpoint()
		panic("why is there a nil url?")
	}
	params := queryParams(u.Query())
	if queryParams.consume(params, SASLEnabled) != "true" {
		return nil, false, nil
	}
	b, ok := r[params.consume(SASLMechanism)]
	if !ok {
		return nil, false, nil
	}
	if err := b.validateParams(params); err != nil {
		return nil, false, err
	}
	mech, err := b.build(params)
	if err != nil {
		return nil, false, err
	}
	// update the url with the consumed params
	u.RawQuery = url.Values(params).Encode()
	return mech, true, nil
}

// TODO: this is mostly a duplication of the changefeedccl.sinkURL stuff. can we share this? move it out into changefeedbase or smth?
type queryParams map[string][]string

func (qp queryParams) peek(key string) (val string) {
	vals, ok := qp[key]
	if !ok || len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func (qp queryParams) consume(key string) string {
	vals, ok := qp[key]
	if !ok || len(vals) == 0 {
		return ""
	}
	delete(qp, key)
	return vals[0]
}

func (qp queryParams) consumeAll(key string) []string {
	vals, ok := qp[key]
	if !ok {
		return nil
	}
	delete(qp, key)
	return vals
}

func newRequiredParamError(mechName string, param string) error {
	return errors.Newf("required parameter %s not provided for %s", param, mechName)
}

func newForbiddenParamError(mechName string, param string) error {
	return errors.Newf("forbidden parameter %s provided for %s", param, mechName)
}

// TODO: embedded common struct? or nah
func peekValidateParams(mechName string, params queryParams, requiredParams, forbiddenParams []string) error {
	var errs []error
	for _, param := range requiredParams {
		if params.peek(param) == "" {
			errs = append(errs, newRequiredParamError(mechName, param))
		}
	}
	for _, param := range forbiddenParams {
		if params.peek(param) != "" {
			errs = append(errs, newForbiddenParamError(mechName, param))
		}
	}
	return errors.Join(errs...)
}

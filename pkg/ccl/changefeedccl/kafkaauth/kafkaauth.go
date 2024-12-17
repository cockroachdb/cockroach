package kafkaauth

import (
	"context"
	"net/url"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type AuthMechanismName string

// TODO: can i not duplicate this here and in the iface?
type matchesFunc func(params queryParams) bool

type authMechanismBuilder interface {
	// TODO: switch to just registering and switching on mechanism name, since this is sasl only now
	matches(params queryParams) bool
	validateParams(params queryParams) error
	build(params queryParams) (AuthMechanism, error)
}

type AuthMechanism interface {
	ApplySarama(ctx context.Context, cfg *sarama.Config) error
	KgoOpts(ctx context.Context) ([]kgo.Opt, error)
}

type AuthMechanismRegistry []authMechanismBuilder

var Registry *AuthMechanismRegistry

func (r *AuthMechanismRegistry) Register(b authMechanismBuilder) {
	*r = append(*r, b)
}

func (r AuthMechanismRegistry) Pick(u *url.URL) (AuthMechanism, bool, error) {
	params := queryParams(u.Query())
	for _, b := range r {
		if b.matches(params) {
			if err := b.validateParams(params); err != nil {
				return nil, false, err
			}
			mech, err := b.build(params)
			if err != nil {
				return nil, false, err
			}
			return mech, true, nil
		}
	}

	// update the url with the consumed params
	u.RawQuery = url.Values(params).Encode()
	return nil, false, nil
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

func newRequiredParamError(mechName AuthMechanismName, param string) error {
	return errors.Newf("required parameter %s not provided for %s", param, mechName)
}

func newForbiddenParamError(mechName AuthMechanismName, param string) error {
	return errors.Newf("forbidden parameter %s provided for %s", param, mechName)
}

// TODO: embedded common struct? or nah
func peekValidateParams(mechName AuthMechanismName, params queryParams, requiredParams, forbiddenParams []string) error {
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

package kafkaauth

import (
	"context"
	"net/url"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type AuthMechanismName string

type PickMeFunc func(params queryParams) (AuthMechanism, bool)

type AuthMechanism interface {
	// TODO: better name
	// TODO: if we're just doing sasl stuff here maybe we can switch on the sasl mechanism name itself, would be simpler
	// the scram one can be separated into 2 structs with a shared common one internally
	// what about azure event hub tho? for that reason let's leave it as is for now.
	PickMe(params queryParams) (AuthMechanism, bool)
	// TODO: what's this for
	Name() AuthMechanismName
	ValidateParams(params queryParams) error
	ApplySarama(ctx context.Context, cfg *sarama.Config) error
	KgoOpts(ctx context.Context) ([]kgo.Opt, error)
}

// peter's suggestion: refactor auth to be plugin/registry based.

// TODO: one per file
// regular sasl mechanisms
// aws
// ""custom""
// azure event hub
// confluent
// others?

type AuthMechanismRegistry []PickMeFunc

var Registry *AuthMechanismRegistry

func (r *AuthMechanismRegistry) Register(f PickMeFunc) {
	*r = append(*r, f)
}

func (r AuthMechanismRegistry) Pick(url *url.URL) (AuthMechanism, bool) {
	for _, f := range r {
		if m, ok := f(queryParams(url.Query())); ok {
			return m, true
		}
	}
	return nil, false
}

type queryParams map[string][]string

// get(key) returns m[key][0] if it exists, else "".
func (qp queryParams) get(key string) (val string) {
	vals, ok := qp[key]
	if !ok || len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func newRequiredParamError(mechName AuthMechanismName, param string) error {
	return errors.Newf("required parameter %s not provided for %s", param, mechName)
}

func newForbiddenParamError(mechName AuthMechanismName, param string) error {
	return errors.Newf("forbidden parameter %s provided for %s", param, mechName)
}

// TODO: embedded common struct? or nah
func validateParams(mechName AuthMechanismName, params queryParams, requiredParams, forbiddenParams []string) error {
	var errs []error
	for _, param := range requiredParams {
		if params.get(param) == "" {
			errs = append(errs, newRequiredParamError(mechName, param))
		}
	}
	for _, param := range forbiddenParams {
		if params.get(param) != "" {
			errs = append(errs, newForbiddenParamError(mechName, param))
		}
	}
	return errors.Join(errs...)
}

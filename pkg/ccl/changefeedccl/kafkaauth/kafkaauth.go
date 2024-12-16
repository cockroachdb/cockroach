package kafkaauth

import (
	"context"
	"errors"
	"net/url"

	"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kgo"
)

type AuthMechanismName string

type PickMeFunc func(params queryParams) (AuthMechanism, bool)

type AuthMechanism interface {
	PickMe(params queryParams) (AuthMechanism, bool)
	Name() AuthMechanismName
	RequiredParams() []string
	ForbiddenParams() []string
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

func newRequiredParamError(param string) error {
	return errors.New("required parameter " + param + " not provided")
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

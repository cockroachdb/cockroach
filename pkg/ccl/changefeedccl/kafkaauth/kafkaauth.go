// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"
	"sort"
	"strings"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type saslMechanismBuilder interface {
	name() string
	validateParams(u *changefeedbase.SinkURL) error
	build(u *changefeedbase.SinkURL) (SASLMechanism, error)
}

// SASLMechanism is an interface for SASL mechanism instances, built from URLs,
// to be applied to sarama and kgo configurations.
type SASLMechanism interface {
	// ApplySarama applies the SASL mechanism to the given sarama configuration.
	ApplySarama(ctx context.Context, cfg *sarama.Config) error
	// KgoOpts returns kgo options that implement the SASL mechanism.
	KgoOpts(ctx context.Context) ([]kgo.Opt, error)
}

type saslMechanismRegistry map[string]saslMechanismBuilder

// registry is the global registry of SASL Mechanisms.
var registry saslMechanismRegistry = make(map[string]saslMechanismBuilder)

// register registers a SASL Mechanism to the global registry. It must only be
// called during init().
func (r saslMechanismRegistry) register(b saslMechanismBuilder) {
	n := b.name()
	if _, ok := r[n]; ok {
		panic("duplicate sasl mechanism registered: " + n)
	}
	r[n] = b
}

// Pick wraps registry.pick() which returns a saslMechanism for the given sink
// URL, or ok=false if none is specified. It consumes all relevant query
// parameters from `u`.
func Pick(u *changefeedbase.SinkURL) (_ SASLMechanism, ok bool, _ error) {
	return registry.pick(u)
}

// pick returns a saslMechanism for the given sink URL, or ok=false if none is specified.
func (r saslMechanismRegistry) pick(u *changefeedbase.SinkURL) (_ SASLMechanism, ok bool, _ error) {
	if u == nil {
		return nil, false, errors.AssertionFailedf("sink url is nil")
	}

	var enabled bool
	if _, err := u.ConsumeBool(changefeedbase.SinkParamSASLEnabled, &enabled); err != nil {
		return nil, false, err
	}
	if !enabled {
		return nil, false, maybeHelpfulErrorMessage(enabled, u)
	}

	mechanism := u.ConsumeParam(changefeedbase.SinkParamSASLMechanism)
	if mechanism == "" {
		mechanism = sarama.SASLTypePlaintext
	}
	b, ok := r[mechanism]
	if !ok {
		return nil, false, errors.Newf("param sasl_mechanism must be one of %s", r.allMechanismNames())
	}

	// Return slightly nicer errors for this common case.
	if b.name() != sarama.SASLTypeOAuth && b.name() != proprietaryOAuthName {
		if err := validateNoOAuthOnlyParams(u); err != nil {
			return nil, false, err
		}
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

func (r saslMechanismRegistry) allMechanismNames() string {
	allMechanisms := make([]string, 0, len(r))
	for k := range r {
		// Exclude PROPRIETARY_OAUTH since we want it to remain undocumented.
		if k == "PROPRIETARY_OAUTH" {
			continue
		}
		allMechanisms = append(allMechanisms, k)
	}
	sort.Strings(allMechanisms)
	return strings.Join(allMechanisms[:len(allMechanisms)-1], ", ") +
		", or " + allMechanisms[len(allMechanisms)-1]
}

func newRequiredParamError(mechName string, param string) error {
	return errors.Newf("%s must be provided when SASL is enabled using mechanism %s", param, mechName)
}

func peekAndRequireParams(
	mechName string, u *changefeedbase.SinkURL, requiredParams []string,
) error {
	var errs []error
	for _, param := range requiredParams {
		if u.PeekParam(param) == "" {
			errs = append(errs, newRequiredParamError(mechName, param))
		}
	}
	return errors.Join(errs...)
}

// consumeHandshake consumes the handshake parameter from the sink URL.
// handshake defaults to true (if sasl is enabled), unlike other options.
func consumeHandshake(u *changefeedbase.SinkURL) (bool, error) {
	var handshake bool
	set, err := u.ConsumeBool(changefeedbase.SinkParamSASLHandshake, &handshake)
	if err != nil {
		return false, err
	}
	if !set {
		handshake = true
	}
	return handshake, nil
}

// maybeHelpfulErrorMessage returns an error if the user has provided SASL parameters without enabling SASL.
func maybeHelpfulErrorMessage(saslEnabled bool, u *changefeedbase.SinkURL) error {
	if !saslEnabled {
		// Handle special error messages.
		if u.PeekParam(changefeedbase.SinkParamSASLHandshake) != "" {
			return errors.New("sasl_enabled must be enabled to configure SASL handshake behavior")
		}
		if u.PeekParam(changefeedbase.SinkParamSASLMechanism) != "" {
			return errors.New("sasl_enabled must be enabled to configure SASL mechanism")
		}

		saslOnlyParams := []string{
			changefeedbase.SinkParamSASLUser,
			changefeedbase.SinkParamSASLPassword,
			changefeedbase.SinkParamSASLEnabled,
			changefeedbase.SinkParamSASLClientID,
			changefeedbase.SinkParamSASLClientSecret,
			changefeedbase.SinkParamSASLTokenURL,
			changefeedbase.SinkParamSASLGrantType,
			changefeedbase.SinkParamSASLScopes,
			changefeedbase.SinkParamSASLAwsIAMRoleArn,
			changefeedbase.SinkParamSASLAwsRegion,
			changefeedbase.SinkParamSASLAwsIAMSessionName,
		}
		for _, p := range saslOnlyParams {
			if u.PeekParam(p) != "" {
				return errors.Newf("sasl_enabled must be enabled if %s is provided", p)
			}
		}
	}
	return nil
}

// validateNoOAuthOnlyParams returns an error if the user has provided
// OAUTHBEARER parameters without setting sasl_mechanism=OAUTHBEARER, for the
// sake of slightly nicer errors.
func validateNoOAuthOnlyParams(u *changefeedbase.SinkURL) error {
	oauthOnlyParams := []string{
		changefeedbase.SinkParamSASLClientID,
		changefeedbase.SinkParamSASLClientSecret,
		changefeedbase.SinkParamSASLTokenURL,
		changefeedbase.SinkParamSASLGrantType,
		changefeedbase.SinkParamSASLScopes,
	}

	for _, p := range oauthOnlyParams {
		if u.PeekParam(p) != "" {
			return errors.Newf("%s is only a valid parameter for sasl_mechanism=OAUTHBEARER", p)
		}
	}
	return nil
}

func applySaramaCommon(cfg *sarama.Config, mechName sarama.SASLMechanism, handshake bool) {
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = mechName
	cfg.Net.SASL.Handshake = handshake
}

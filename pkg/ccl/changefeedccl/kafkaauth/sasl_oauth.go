// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"
	"net/url"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosasloauth "github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type saslOAuthBearerBuilder struct{}

// name implements authMechanismBuilder.
func (s saslOAuthBearerBuilder) name() string {
	return sarama.SASLTypeOAuth
}

// validateParams implements authMechanismBuilder.
func (s saslOAuthBearerBuilder) validateParams(u *changefeedbase.SinkURL) error {
	requiredParams := []string{changefeedbase.SinkParamSASLClientID, changefeedbase.SinkParamSASLClientSecret, changefeedbase.SinkParamSASLTokenURL}
	return peekAndRequireParams(sarama.SASLTypeOAuth, u, requiredParams)
}

// build implements authMechanismBuilder.
func (s saslOAuthBearerBuilder) build(u *changefeedbase.SinkURL) (SASLMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}
	var clientSecret []byte
	if err := u.DecodeBase64(changefeedbase.SinkParamSASLClientSecret, &clientSecret); err != nil {
		return nil, errors.Wrap(err, "decoding client secret")
	}
	return &saslOAuthBearer{
		clientID:     u.ConsumeParam(changefeedbase.SinkParamSASLClientID),
		clientSecret: clientSecret,
		tokenURL:     u.ConsumeParam(changefeedbase.SinkParamSASLTokenURL),
		grantType:    u.ConsumeParam(changefeedbase.SinkParamSASLGrantType),
		scopes:       u.ConsumeParams(changefeedbase.SinkParamSASLScopes),
		handshake:    handshake,
	}, nil
}

var _ saslMechanismBuilder = saslOAuthBearerBuilder{}

type saslOAuthBearer struct {
	clientID     string
	clientSecret []byte
	tokenURL     string
	grantType    string
	scopes       []string
	handshake    bool
}

// ApplySarama implements AuthMechanism.
func (s *saslOAuthBearer) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	tp, err := s.newSaramaTokenProvider(ctx)
	if err != nil {
		return err
	}
	applySaramaCommon(cfg, sarama.SASLTypeOAuth, s.handshake)
	cfg.Net.SASL.TokenProvider = tp
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *saslOAuthBearer) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	tp, err := s.newKgoTokenProvider(ctx)
	if err != nil {
		return nil, err
	}

	return []kgo.Opt{kgo.SASL(kgosasloauth.Oauth(tp))}, nil
}

func (s *saslOAuthBearer) newSaramaTokenProvider(
	ctx context.Context,
) (sarama.AccessTokenProvider, error) {
	ts, err := s.makeTokenSource(ctx)
	if err != nil {
		return nil, err
	}
	return &saramaOauthTokenProvider{tokenSource: ts}, nil
}

func (s *saslOAuthBearer) newKgoTokenProvider(
	ctx context.Context,
) (func(ctx context.Context) (kgosasloauth.Auth, error), error) {
	ts, err := s.makeTokenSource(ctx)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) (kgosasloauth.Auth, error) {
		tok, err := ts.Token()
		if err != nil {
			return kgosasloauth.Auth{}, err
		}
		return kgosasloauth.Auth{Token: tok.AccessToken}, nil
	}, nil

}

func (s *saslOAuthBearer) makeTokenSource(ctx context.Context) (oauth2.TokenSource, error) {
	// grant_type is by default going to be set to 'client_credentials' by the
	// clientcredentials library as defined by the spec, however non-compliant
	// auth server implementations may want a custom type
	var endpointParams url.Values
	if s.grantType != `` {
		endpointParams = url.Values{"grant_type": {s.grantType}}
	}

	tokenURL, err := url.Parse(s.tokenURL)
	if err != nil {
		return nil, errors.Wrap(err, "malformed token url")
	}

	// the clientcredentials.Config's TokenSource method creates an
	// oauth2.TokenSource implementation which returns tokens for the given
	// endpoint, returning the same cached result until its expiration has been
	// reached, and then once expired re-requesting a new token from the endpoint.
	cfg := clientcredentials.Config{
		ClientID:       s.clientID,
		ClientSecret:   string(s.clientSecret),
		TokenURL:       tokenURL.String(),
		Scopes:         s.scopes,
		EndpointParams: endpointParams,
	}
	return cfg.TokenSource(ctx), nil
}

var _ SASLMechanism = (*saslOAuthBearer)(nil)

type saramaOauthTokenProvider struct {
	tokenSource oauth2.TokenSource
}

var _ sarama.AccessTokenProvider = (*saramaOauthTokenProvider)(nil)

// Token implements the sarama.AccessTokenProvider interface.  This is called by
// Sarama when connecting to the broker.
func (t *saramaOauthTokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		// Errors will result in Sarama retrying the broker connection and logging
		// the transient error, with a Broker connection error surfacing after retry
		// attempts have been exhausted.
		return nil, err
	}

	return &sarama.AccessToken{Token: token.AccessToken}, nil
}

func init() {
	registry.register(saslOAuthBearerBuilder{})
}

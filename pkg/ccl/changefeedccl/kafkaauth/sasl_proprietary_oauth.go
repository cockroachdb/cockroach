// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosasloauth "github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
)

const proprietaryOAuthName = "PROPRIETARY_OAUTH"

type saslProprietaryOAuthBuilder struct{}

// name implements authMechanismBuilder.
func (s saslProprietaryOAuthBuilder) name() string {
	return proprietaryOAuthName
}

// validateParams implements authMechanismBuilder.
func (s saslProprietaryOAuthBuilder) validateParams(u *changefeedbase.SinkURL) error {
	requiredParams := []string{
		changefeedbase.SinkParamSASLClientID,
		changefeedbase.SinkParamSASLTokenURL,
		changefeedbase.SinkParamSASLProprietaryResource,
		changefeedbase.SinkParamSASLProprietaryClientAssertion,
		changefeedbase.SinkParamSASLProprietaryClientAssertionType,
	}
	return peekAndRequireParams(s.name(), u, requiredParams)
}

// build implements authMechanismBuilder.
func (s saslProprietaryOAuthBuilder) build(u *changefeedbase.SinkURL) (SASLMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}
	return &saslProprietaryOAuth{
		clientID:            u.ConsumeParam(changefeedbase.SinkParamSASLClientID),
		tokenURL:            u.ConsumeParam(changefeedbase.SinkParamSASLTokenURL),
		resource:            u.ConsumeParam(changefeedbase.SinkParamSASLProprietaryResource),
		clientAssertion:     u.ConsumeParam(changefeedbase.SinkParamSASLProprietaryClientAssertion),
		clientAssertionType: u.ConsumeParam(changefeedbase.SinkParamSASLProprietaryClientAssertionType),
		handshake:           handshake,
	}, nil
}

var _ saslMechanismBuilder = saslProprietaryOAuthBuilder{}

type saslProprietaryOAuth struct {
	clientID, tokenURL, resource,
	clientAssertion, clientAssertionType string
	handshake bool
}

// ApplySarama implements AuthMechanism.
func (s *saslProprietaryOAuth) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	tp, err := s.newSaramaTokenProvider(ctx)
	if err != nil {
		return err
	}
	applySaramaCommon(cfg, sarama.SASLTypeOAuth, s.handshake)
	cfg.Net.SASL.TokenProvider = tp
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *saslProprietaryOAuth) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	tp, err := s.newKgoTokenProvider(ctx)
	if err != nil {
		return nil, err
	}

	return []kgo.Opt{kgo.SASL(kgosasloauth.Oauth(tp))}, nil
}

func (s *saslProprietaryOAuth) newSaramaTokenProvider(
	ctx context.Context,
) (sarama.AccessTokenProvider, error) {
	return &saramaOauthTokenProvider{tokenSource: s.newTokenSource(ctx)}, nil
}

func (s *saslProprietaryOAuth) newKgoTokenProvider(
	ctx context.Context,
) (func(ctx context.Context) (kgosasloauth.Auth, error), error) {
	ts := oauth2.ReuseTokenSource(nil, s.newTokenSource(ctx))
	return func(ctx context.Context) (kgosasloauth.Auth, error) {
		tok, err := ts.Token()
		if err != nil {
			return kgosasloauth.Auth{}, err
		}
		return kgosasloauth.Auth{Token: tok.AccessToken}, nil
	}, nil
}

func (s *saslProprietaryOAuth) newTokenSource(ctx context.Context) oauth2.TokenSource {
	return proprietaryTokenSource{
		tokenURL:            s.tokenURL,
		clientID:            s.clientID,
		clientAssertion:     s.clientAssertion,
		clientAssertionType: s.clientAssertionType,
		resource:            s.resource,
		ctx:                 ctx,
		client:              &http.Client{},
	}
}

var _ SASLMechanism = (*saslProprietaryOAuth)(nil)

type proprietaryTokenSource struct {
	tokenURL, clientID, clientAssertion, clientAssertionType, resource string
	// The oauth2.TokenSource API seems to require us to keep a context in here.
	ctx    context.Context
	client *http.Client
}

// Token implements the oauth2.TokenSource interface.
func (s proprietaryTokenSource) Token() (*oauth2.Token, error) {
	tokenURL, err := url.Parse(s.tokenURL)
	if err != nil {
		return nil, errors.Wrap(err, "malformed token url")
	}

	bodyParams := url.Values{
		"grant_type":            {"client_credentials"},
		"client_id":             {s.clientID},
		"client_assertion_type": {s.clientAssertionType},
		"client_assertion":      {s.clientAssertion},
		"resource":              {s.resource},
	}

	req, err := http.NewRequestWithContext(s.ctx, "POST", tokenURL.String(), strings.NewReader(bodyParams.Encode()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create oauth token request")
	}
	req.Header.Set("Content-Type", "application/www-url-encoded")

	res, err := s.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to make oauth token request")
	}

	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, errors.Join(errors.Wrap(err, "failed to read oauth response body"), res.Body.Close())
	}
	if err := res.Body.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to close oauth response body")
	}

	var resp proprietaryOAuthResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, errors.Wrapf(err, "failed to parse oauth response")
	}
	if resp.AccessToken == "" {
		return nil, errors.Errorf("no access token in oauth response")
	}

	tok := &oauth2.Token{AccessToken: resp.AccessToken, TokenType: resp.TokenType}

	if resp.ExpiresIn > 0 {
		tok.Expiry = timeutil.Now().Add(time.Duration(resp.ExpiresIn) * time.Second)
	}

	return tok, nil
}

var _ oauth2.TokenSource = proprietaryTokenSource{}

type proprietaryOAuthResp struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

func init() {
	registry.register(saslProprietaryOAuthBuilder{})
}

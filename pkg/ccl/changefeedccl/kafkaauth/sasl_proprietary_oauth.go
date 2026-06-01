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

// name implements saslMechanismBuilder.
func (s saslProprietaryOAuthBuilder) name() string {
	return proprietaryOAuthName
}

// validateParams implements saslMechanismBuilder.
func (s saslProprietaryOAuthBuilder) validateParams(u *changefeedbase.SinkURL) error {
	requiredParams := []string{
		changefeedbase.SinkParamSASLClientID,
		changefeedbase.SinkParamSASLTokenURL,
		changefeedbase.SinkParamSASLProprietaryResource,
		changefeedbase.SinkParamSASLProprietaryClientAssertionType,
	}
	if err := peekAndRequireParams(s.name(), u, requiredParams); err != nil {
		return err
	}
	hasAssertion := u.PeekParam(changefeedbase.SinkParamSASLProprietaryClientAssertion) != ""
	hasAssertionLocation := u.PeekParam(changefeedbase.SinkParamSASLProprietaryClientAssertionLocation) != ""
	switch {
	case hasAssertion && hasAssertionLocation:
		return errors.Newf("%s and %s cannot be used together",
			changefeedbase.SinkParamSASLProprietaryClientAssertion,
			changefeedbase.SinkParamSASLProprietaryClientAssertionLocation)
	case !hasAssertion && !hasAssertionLocation:
		return errors.Newf("one of %s or %s must be provided when SASL is enabled using mechanism %s",
			changefeedbase.SinkParamSASLProprietaryClientAssertion,
			changefeedbase.SinkParamSASLProprietaryClientAssertionLocation,
			proprietaryOAuthName)
	}
	return nil
}

// build implements saslMechanismBuilder.
func (s saslProprietaryOAuthBuilder) build(
	u *changefeedbase.SinkURL, cfg SASLConfig,
) (SASLMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}
	getClientAssertion, err := buildClientAssertionFn(u, cfg)
	if err != nil {
		return nil, err
	}
	return &saslProprietaryOAuth{
		clientID:            u.ConsumeParam(changefeedbase.SinkParamSASLClientID),
		tokenURL:            u.ConsumeParam(changefeedbase.SinkParamSASLTokenURL),
		resource:            u.ConsumeParam(changefeedbase.SinkParamSASLProprietaryResource),
		clientAssertionType: u.ConsumeParam(changefeedbase.SinkParamSASLProprietaryClientAssertionType),
		getClientAssertion:  getClientAssertion,
		handshake:           handshake,
	}, nil
}

func buildClientAssertionFn(
	u *changefeedbase.SinkURL, cfg SASLConfig,
) (func() (string, error), error) {
	if loc := u.ConsumeParam(changefeedbase.SinkParamSASLProprietaryClientAssertionLocation); loc != "" {
		path, err := cfg.ExternalCredentialsDir.Resolve(loc)
		if err != nil {
			return nil, errors.Wrapf(err, "resolving %s on n%d",
				changefeedbase.SinkParamSASLProprietaryClientAssertionLocation,
				cfg.SQLInstanceID)
		}
		return func() (string, error) {
			b, err := path.Read()
			if err != nil {
				return "", errors.Wrapf(err, "reading client assertion file %q on n%d", path, cfg.SQLInstanceID)
			}
			return strings.TrimSpace(string(b)), nil
		}, nil
	}
	assertion := u.ConsumeParam(changefeedbase.SinkParamSASLProprietaryClientAssertion)
	return func() (string, error) { return assertion, nil }, nil
}

var _ saslMechanismBuilder = saslProprietaryOAuthBuilder{}

type saslProprietaryOAuth struct {
	clientID, tokenURL, resource, clientAssertionType string
	getClientAssertion                                func() (string, error)

	handshake bool
}

// ApplySarama implements SASLMechanism.
func (s *saslProprietaryOAuth) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	tp, err := s.newSaramaTokenProvider(ctx)
	if err != nil {
		return err
	}
	applySaramaCommon(cfg, sarama.SASLTypeOAuth, s.handshake)
	cfg.Net.SASL.TokenProvider = tp
	return nil
}

// KgoOpts implements SASLMechanism.
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
		getClientAssertion:  s.getClientAssertion,
		clientAssertionType: s.clientAssertionType,
		resource:            s.resource,
		ctx:                 ctx,
		client:              &http.Client{},
	}
}

var _ SASLMechanism = (*saslProprietaryOAuth)(nil)

type proprietaryTokenSource struct {
	tokenURL, clientID, clientAssertionType, resource string
	getClientAssertion                                func() (string, error)
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

	clientAssertion, err := s.getClientAssertion()
	if err != nil {
		return nil, err
	}

	bodyParams := url.Values{
		"grant_type":            {"client_credentials"},
		"client_id":             {s.clientID},
		"client_assertion_type": {s.clientAssertionType},
		"client_assertion":      {clientAssertion},
		"resource":              {s.resource},
	}

	req, err := http.NewRequestWithContext(s.ctx, "POST", tokenURL.String(), strings.NewReader(bodyParams.Encode()))
	if err != nil {
		return nil, errors.Wrap(err, "creating oauth token request")
	}
	req.Header.Set("Content-Type", "application/www-url-encoded")

	res, err := s.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "issuing oauth token request")
	}

	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, errors.Join(errors.Wrap(err, "reading oauth response body"), res.Body.Close())
	}
	if err := res.Body.Close(); err != nil {
		return nil, errors.Wrap(err, "closing oauth response body")
	}

	var resp proprietaryOAuthResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, errors.Wrap(err, "parsing oauth response")
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

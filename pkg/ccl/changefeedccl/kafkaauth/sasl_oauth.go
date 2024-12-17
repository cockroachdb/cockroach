package kafkaauth

import (
	"context"
	"net/url"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	kgosasloauth "github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type saslOAuthBearer struct {
	clientID     string
	clientSecret string
	tokenURL     string
	grantType    string
	scopes       []string
	handshake    bool
}

// PickMe implements AuthMechanism.
func (s *saslOAuthBearer) PickMe(params queryParams) (AuthMechanism, bool) {
	if params.get(SASLEnabled) == "true" && params.get(SASLMechanism) == sarama.SASLTypeOAuth {
		return &saslOAuthBearer{
			clientID:     params.get(SASLClientID),
			clientSecret: params.get(SASLClientSecret), // TODO: decode b64
			tokenURL:     params.get(SASLTokenURL),
			grantType:    params.get(SASLGrantType),
			scopes:       params[SASLScopes],
			handshake:    params.get(SASLHandshake) == "" || params.get(SASLHandshake) == "true",
		}, true
	}
	return nil, false
}

// ValidateParams implements AuthMechanism.
func (s *saslOAuthBearer) ValidateParams(params queryParams) error {
	requiredParams := []string{SASLClientID, SASLClientSecret, SASLTokenURL}
	return validateParams(s.Name(), params, requiredParams, nil)
}

// ApplySarama implements AuthMechanism.
func (s *saslOAuthBearer) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	tp, err := s.newSaramaTokenProvider(ctx)
	if err != nil {
		return err
	}
	cfg.Net.SASL.Enable = true
	// TODO: commonize handshake, enable, mechanism(?)
	cfg.Net.SASL.Handshake = s.handshake
	cfg.Net.SASL.Mechanism = sarama.SASLTypeOAuth
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

// Name implements AuthMechanism.
func (s *saslOAuthBearer) Name() AuthMechanismName {
	return "SASL_OAUTHBEARER"
}

func (s *saslOAuthBearer) newSaramaTokenProvider(ctx context.Context) (sarama.AccessTokenProvider, error) {
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
		ClientSecret:   s.clientSecret,
		TokenURL:       tokenURL.String(),
		Scopes:         s.scopes,
		EndpointParams: endpointParams,
	}
	return &saramaOauthTokenProvider{
		tokenSource: cfg.TokenSource(ctx),
	}, nil
}

func (s *saslOAuthBearer) newKgoTokenProvider(ctx context.Context) (func(ctx context.Context) (kgosasloauth.Auth, error), error) {
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
		ClientSecret:   s.clientSecret,
		TokenURL:       tokenURL.String(),
		Scopes:         s.scopes,
		EndpointParams: endpointParams,
	}
	ts := cfg.TokenSource(ctx)

	return func(ctx context.Context) (kgosasloauth.Auth, error) {
		tok, err := ts.Token()
		if err != nil {
			return kgosasloauth.Auth{}, err
		}
		return kgosasloauth.Auth{Token: tok.AccessToken}, nil
	}, nil

}

var _ AuthMechanism = (*saslOAuthBearer)(nil)

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
	Registry.Register((&saslOAuthBearer{}).PickMe)
}

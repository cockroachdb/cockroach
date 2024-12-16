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

type SASLOAuthBearer struct {
	clientID     string
	clientSecret string
	tokenURL     string
	grantType    string
	scopes       []string
}

// PickMe implements AuthMechanism.
func (s *SASLOAuthBearer) PickMe(params queryParams) (AuthMechanism, bool) {
	if params.get(SASLEnabled) == "true" && params.get(SASLMechanism) == "OAUTHBEARER" {
		return &SASLOAuthBearer{
			clientID:     params.get(SASLClientID),
			clientSecret: params.get(SASLClientSecret), // TODO: decode b64
			tokenURL:     params.get(SASLTokenURL),
			grantType:    params.get(SASLGrantType),
			scopes:       params[SASLScopes],
		}, true
	}
	return nil, false
}

// RequiredParams implements AuthMechanism.
func (s *SASLOAuthBearer) RequiredParams() []string {
	return []string{SASLClientID, SASLClientSecret}
}

// ForbiddenParams implements AuthMechanism.
func (s *SASLOAuthBearer) ForbiddenParams() []string {
	// TODO
	return []string{}
}

// ValidateParams implements AuthMechanism.
func (s *SASLOAuthBearer) ValidateParams(params queryParams) error {
	var errs []error
	if params.get(SASLClientID) == "" {
		errs = append(errs, newRequiredParamError(SASLClientID))
	}
	if params.get(SASLClientSecret) == "" {
		errs = append(errs, newRequiredParamError(SASLClientSecret))
	}
	return errors.Join(errs...)
}

// ApplySarama implements AuthMechanism.
func (s *SASLOAuthBearer) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	tp, err := s.newSaramaTokenProvider(ctx)
	if err != nil {
		return err
	}
	cfg.Net.SASL.TokenProvider = tp
	return nil
}

// KgoOpts implements AuthMechanism.
func (s *SASLOAuthBearer) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	tp, err := s.newKgoTokenProvider(ctx)
	if err != nil {
		return nil, err
	}

	return []kgo.Opt{kgo.SASL(kgosasloauth.Oauth(tp))}, nil
}

// Name implements AuthMechanism.
func (s *SASLOAuthBearer) Name() AuthMechanismName {
	return "SASL_OAUTHBEARER"
}

func (s *SASLOAuthBearer) newSaramaTokenProvider(ctx context.Context) (sarama.AccessTokenProvider, error) {
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
	return &saramaTokenProvider{
		tokenSource: cfg.TokenSource(ctx),
	}, nil
}

func (s *SASLOAuthBearer) newKgoTokenProvider(ctx context.Context) (func(ctx context.Context) (kgosasloauth.Auth, error), error) {
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

var _ AuthMechanism = (*SASLOAuthBearer)(nil)

type saramaTokenProvider struct {
	tokenSource oauth2.TokenSource
}

var _ sarama.AccessTokenProvider = (*saramaTokenProvider)(nil)

// Token implements the sarama.AccessTokenProvider interface.  This is called by
// Sarama when connecting to the broker.
func (t *saramaTokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		// Errors will result in Sarama retrying the broker connection and logging
		// the transient error, with a Broker connection error surfacing after retry
		// attempts have been exhausted.
		return nil, err
	}

	return &sarama.AccessToken{Token: token.AccessToken}, nil
}

// type awsIAMRoleSASLTokenProvider struct {
// 	ctx            context.Context
// 	awsRegion      string
// 	iamRoleArn     string
// 	iamSessionName string
// }

// func (p *awsIAMRoleSASLTokenProvider) Token() (*sarama.AccessToken, error) {
// 	token, _, err := signer.GenerateAuthTokenFromRole(
// 		p.ctx, p.awsRegion, p.iamRoleArn, p.iamSessionName)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &sarama.AccessToken{Token: token}, nil
// }

// func newAwsIAMRoleSASLTokenProvider(
// 	ctx context.Context, awsRegion, iamRoleArn, iamSessionName string,
// ) (sarama.AccessTokenProvider, error) {
// 	return &awsIAMRoleSASLTokenProvider{
// 		ctx:            ctx,
// 		awsRegion:      awsRegion,
// 		iamRoleArn:     iamRoleArn,
// 		iamSessionName: iamSessionName,
// 	}, nil
// }

func init() {
	Registry.Register((&SASLOAuthBearer{}).PickMe)
}

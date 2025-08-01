// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"
	"os"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/errors"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
)

type saslKerberosBuilder struct{}

// name implements saslMechanismBuilder.
func (s saslKerberosBuilder) name() string {
	return sarama.SASLTypeGSSAPI
}

// validateParams implements saslMechanismBuilder.
func (s saslKerberosBuilder) validateParams(u *changefeedbase.SinkURL) error {
	// Service name is required for Kerberos authentication
	requiredParams := []string{changefeedbase.SinkParamSASLKerberosServiceName}

	if err := peekAndRequireParams(s.name(), u, requiredParams); err != nil {
		return err
	}

	// Validate that we have either principal+keytab or rely on ticket cache
	principal := u.PeekParam(changefeedbase.SinkParamSASLKerberosPrincipal)
	keytabPath := u.PeekParam(changefeedbase.SinkParamSASLKerberosKeytabPath)

	// If principal is provided, keytab must also be provided
	if principal != "" && keytabPath == "" {
		return errors.Newf("sasl_kerberos_keytab_path must be provided when sasl_kerberos_principal is specified")
	}

	// If keytab is provided, principal must also be provided
	if keytabPath != "" && principal == "" {
		return errors.Newf("sasl_kerberos_principal must be provided when sasl_kerberos_keytab_path is specified")
	}

	// If keytab path is provided, verify the file exists
	if keytabPath != "" {
		if _, err := os.Stat(keytabPath); os.IsNotExist(err) {
			return errors.Newf("keytab file not found at path %s", keytabPath)
		}
	}

	return nil
}

// build implements saslMechanismBuilder.
func (s saslKerberosBuilder) build(u *changefeedbase.SinkURL) (SASLMechanism, error) {
	handshake, err := consumeHandshake(u)
	if err != nil {
		return nil, err
	}

	return &saslKerberos{
		serviceName: u.ConsumeParam(changefeedbase.SinkParamSASLKerberosServiceName),
		realm:       u.ConsumeParam(changefeedbase.SinkParamSASLKerberosRealm),
		principal:   u.ConsumeParam(changefeedbase.SinkParamSASLKerberosPrincipal),
		keytabPath:  u.ConsumeParam(changefeedbase.SinkParamSASLKerberosKeytabPath),
		configPath:  u.ConsumeParam(changefeedbase.SinkParamSASLKerberosConfig),
		handshake:   handshake,
	}, nil
}

var _ saslMechanismBuilder = saslKerberosBuilder{}

type saslKerberos struct {
	serviceName string
	realm       string
	principal   string
	keytabPath  string
	configPath  string
	handshake   bool
}

// ApplySarama implements SASLMechanism.
func (s *saslKerberos) ApplySarama(ctx context.Context, cfg *sarama.Config) error {
	applySaramaCommon(cfg, sarama.SASLTypeGSSAPI, s.handshake)

	// Configure GSSAPI-specific settings
	cfg.Net.SASL.GSSAPI.ServiceName = s.serviceName
	if s.realm != "" {
		cfg.Net.SASL.GSSAPI.Realm = s.realm
	}

	// Set up authentication method
	if s.principal != "" && s.keytabPath != "" {
		// Use keytab-based authentication
		cfg.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
		cfg.Net.SASL.GSSAPI.Username = s.principal
		cfg.Net.SASL.GSSAPI.KeyTabPath = s.keytabPath
	} else {
		// Use ticket cache authentication (kinit)
		cfg.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
	}

	if s.configPath != "" {
		cfg.Net.SASL.GSSAPI.KerberosConfigPath = s.configPath
	}

	return nil
}

// KgoOpts implements SASLMechanism.
func (s *saslKerberos) KgoOpts(ctx context.Context) ([]kgo.Opt, error) {
	// Create kerberos authentication mechanism using franz-go's kerberos package
	authFn := func(authCtx context.Context) (kerberos.Auth, error) {
		auth := kerberos.Auth{
			Service: s.serviceName,
		}

		// Configure based on available parameters
		if s.principal != "" && s.keytabPath != "" {
			// Use keytab-based authentication
			auth.ClientFn = func(clientCtx context.Context) *client.Client {
				return s.createKerberosClientWithKeytab(clientCtx)
			}
		} else {
			// Use default credential cache authentication
			auth.ClientFn = func(clientCtx context.Context) *client.Client {
				return s.createKerberosClientWithCache(clientCtx)
			}
		}

		// Enable persistence for reuse across connections
		auth.PersistAfterAuth = true

		return auth, nil
	}

	// Create the SASL mechanism
	mechanism := kerberos.Kerberos(authFn)

	return []kgo.Opt{kgo.SASL(mechanism)}, nil
}

// createKerberosClientWithKeytab creates a Kerberos client using keytab authentication
func (s *saslKerberos) createKerberosClientWithKeytab(ctx context.Context) *client.Client {
	// Create configuration
	cfg := config.New()
	if s.realm != "" {
		cfg.LibDefaults.DefaultRealm = s.realm
	}
	if s.configPath != "" {
		var err error
		cfg, err = config.Load(s.configPath)
		if err != nil {
			// Fall back to default config if loading fails
			cfg = config.New()
		}
	}

	// Load keytab
	kt, err := keytab.Load(s.keytabPath)
	if err != nil {
		return nil
	}

	// Create client with keytab
	cl := client.NewWithKeytab(s.principal, s.realm, kt, cfg)
	return cl
}

// createKerberosClientWithCache creates a Kerberos client using credential cache
func (s *saslKerberos) createKerberosClientWithCache(ctx context.Context) *client.Client {
	// Create configuration
	cfg := config.New()
	if s.realm != "" {
		cfg.LibDefaults.DefaultRealm = s.realm
	}
	if s.configPath != "" {
		var err error
		cfg, err = config.Load(s.configPath)
		if err != nil {
			// Fall back to default config if loading fails
			cfg = config.New()
		}
	}

	// Create client with credential cache (default)
	cl := client.NewWithPassword("", s.realm, "", cfg)
	return cl
}

var _ SASLMechanism = (*saslKerberos)(nil)

func init() {
	registry.register(saslKerberosBuilder{})
}

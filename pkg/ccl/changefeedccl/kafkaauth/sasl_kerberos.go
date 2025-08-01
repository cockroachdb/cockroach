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
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
)

// Required parameters for Kerberos SASL authentication
var requiredParamsSASLKerberos = []string{changefeedbase.SinkParamSASLKerberosServiceName}

type saslKerberosBuilder struct{}

// name implements saslMechanismBuilder.
func (s saslKerberosBuilder) name() string {
	return sarama.SASLTypeGSSAPI
}

// validateParams implements saslMechanismBuilder.
func (s saslKerberosBuilder) validateParams(u *changefeedbase.SinkURL) error {
	if err := peekAndRequireParams(s.name(), u, requiredParamsSASLKerberos); err != nil {
		return err
	}

	// Validate that we have either principal+keytab or rely on ticket cache
	principal := u.PeekParam(changefeedbase.SinkParamSASLKerberosPrincipal)
	keytabPath := u.PeekParam(changefeedbase.SinkParamSASLKerberosKeytabPath)

	// If principal is provided, keytab must also be provided, and vice versa.
	if principal != "" && keytabPath == "" {
		return errors.Newf("sasl_kerberos_keytab_path must be provided when sasl_kerberos_principal is specified")
	}
	if keytabPath != "" && principal == "" {
		return errors.Newf("sasl_kerberos_principal must be provided when sasl_kerberos_keytab_path is specified")
	}

	// If keytab path is provided, verify the file exists.
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
		serviceName:   u.ConsumeParam(changefeedbase.SinkParamSASLKerberosServiceName),
		realm:         u.ConsumeParam(changefeedbase.SinkParamSASLKerberosRealm),
		principal:     u.ConsumeParam(changefeedbase.SinkParamSASLKerberosPrincipal),
		keytabPath:    u.ConsumeParam(changefeedbase.SinkParamSASLKerberosKeytabPath),
		configPath:    u.ConsumeParam(changefeedbase.SinkParamSASLKerberosConfig),
		handshake:     handshake,
		clientFactory: defaultKerberosClientFactory{},
	}, nil
}

var _ saslMechanismBuilder = saslKerberosBuilder{}

// kerberosClientFactory creates Kerberos clients for different authentication methods
type kerberosClientFactory interface {
	createWithKeytab(ctx context.Context, principal, realm, keytabPath, configPath string) (*client.Client, error)
	createWithCache(ctx context.Context, realm, configPath string) (*client.Client, error)
}

// defaultKerberosClientFactory implements kerberosClientFactory using real gokrb5 clients
type defaultKerberosClientFactory struct{}

func (f defaultKerberosClientFactory) createWithKeytab(ctx context.Context, principal, realm, keytabPath, configPath string) (*client.Client, error) {
	// Create configuration
	cfg := config.New()
	if realm != "" {
		cfg.LibDefaults.DefaultRealm = realm
	}
	if configPath != "" {
		var err error
		cfg, err = config.Load(configPath)
		if err != nil {
			return nil, errors.Wrap(err, "loading config")
		}
	}

	// Load keytab
	kt, err := keytab.Load(keytabPath)
	if err != nil {
		return nil, errors.Wrap(err, "loading keytab")
	}

	// Create client with keytab
	cl := client.NewWithKeytab(principal, realm, kt, cfg)
	return cl, nil
}

func (f defaultKerberosClientFactory) createWithCache(ctx context.Context, realm, configPath string) (*client.Client, error) {
	// Create configuration
	cfg := config.New()
	if realm != "" {
		cfg.LibDefaults.DefaultRealm = realm
	}
	if configPath != "" {
		var err error
		cfg, err = config.Load(configPath)
		if err != nil {
			return nil, errors.Wrap(err, "loading config")
		}
	}

	// Load credential cache from default location or KRB5CCNAME
	ccachePath := os.Getenv("KRB5CCNAME")
	if ccachePath == "" {
		// Default to system default credential cache location
		ccachePath = ""
	}
	cc, err := credentials.LoadCCache(ccachePath)
	if err != nil {
		return nil, errors.Wrap(err, "loading credential cache")
	}

	// Create client with credential cache
	cl, err := client.NewFromCCache(cc, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "creating client from credential cache")
	}
	return cl, nil
}

type saslKerberos struct {
	serviceName   string
	realm         string
	principal     string
	keytabPath    string
	configPath    string
	handshake     bool
	clientFactory kerberosClientFactory // for testing
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
	// Create kerberos client upfront so we can return errors properly
	var client *client.Client
	var err error

	// Configure based on available parameters
	if s.principal != "" && s.keytabPath != "" {
		// Use keytab-based authentication
		client, err = s.clientFactory.createWithKeytab(ctx, s.principal, s.realm, s.keytabPath, s.configPath)
		if err != nil {
			return nil, err
		}
	} else {
		// Use default credential cache authentication
		client, err = s.clientFactory.createWithCache(ctx, s.realm, s.configPath)
		if err != nil {
			return nil, err
		}
	}

	auth := kerberos.Auth{
		Service:          s.serviceName,
		Client:           client,
		PersistAfterAuth: true,
	}

	return []kgo.Opt{kgo.SASL(auth.AsMechanismWithClose())}, nil
}

var _ SASLMechanism = (*saslKerberos)(nil)

func init() {
	registry.register(saslKerberosBuilder{})
}

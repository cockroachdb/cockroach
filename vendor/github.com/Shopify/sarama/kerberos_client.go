package sarama

import (
	krb5client "github.com/jcmturner/gokrb5/v8/client"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/types"
)

type KerberosGoKrb5Client struct {
	krb5client.Client
}

func (c *KerberosGoKrb5Client) Domain() string {
	return c.Credentials.Domain()
}

func (c *KerberosGoKrb5Client) CName() types.PrincipalName {
	return c.Credentials.CName()
}

// NewKerberosClient creates kerberos client used to obtain TGT and TGS tokens.
// It uses pure go Kerberos 5 solution (RFC-4121 and RFC-4120).
// uses gokrb5 library underlying which is a pure go kerberos client with some GSS-API capabilities.
func NewKerberosClient(config *GSSAPIConfig) (KerberosClient, error) {
	cfg, err := krb5config.Load(config.KerberosConfigPath)
	if err != nil {
		return nil, err
	}
	return createClient(config, cfg)
}

func createClient(config *GSSAPIConfig, cfg *krb5config.Config) (KerberosClient, error) {
	var client *krb5client.Client
	if config.AuthType == KRB5_KEYTAB_AUTH {
		kt, err := keytab.Load(config.KeyTabPath)
		if err != nil {
			return nil, err
		}
		client = krb5client.NewWithKeytab(config.Username, config.Realm, kt, cfg, krb5client.DisablePAFXFAST(config.DisablePAFXFAST))
	} else {
		client = krb5client.NewWithPassword(config.Username,
			config.Realm, config.Password, cfg, krb5client.DisablePAFXFAST(config.DisablePAFXFAST))
	}
	return &KerberosGoKrb5Client{*client}, nil
}

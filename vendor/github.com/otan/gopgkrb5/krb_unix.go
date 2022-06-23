//go:build !windows
// +build !windows

package gopgkrb5

import (
	"fmt"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"os"
	"os/user"
	"strings"
)

/*
 * UNIX Kerberos support, using jcmturner's pure-go
 * implementation
 *
 * Keytab support is available only on unix systems
 */

// GSS implements the pq.GSS interface and the pgconn.GSS interface.
type GSS struct {
	cli    *client.Client
	ktPath string
	realm  string
	spn    string
}

// NewGSS creates a new GSS provider.
func NewGSS() (*GSS, error) {
	g := &GSS{}
	err := g.init()

	if err != nil {
		return nil, err
	}

	return g, nil
}

func NewGSSWithKeytab(spn string, realm string, ktPath string) (*GSS, error) {
	g := &GSS{}
	g.ktPath = ktPath
	g.spn = spn
	g.realm = realm

	err := g.init()

	if err != nil {
		return nil, err
	}

	return g, nil
}

func (g *GSS) init() error {
	cfgPath, ok := os.LookupEnv("KRB5_CONFIG")
	if !ok {
		cfgPath = "/etc/krb5.conf"
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	u, err := user.Current()
	if err != nil {
		return err
	}

	var cl *client.Client

	// If we have keytab path set, we create client from keytab
	// Otherwise, we use ccache file
	if g.ktPath != "" {
		kt, err := keytab.Load(g.ktPath)
		if err != nil {
			panic(err)
		}

		cl = client.NewWithKeytab(g.spn, g.realm, kt, cfg)
	} else {
		ccpath := "/tmp/krb5cc_" + u.Uid

		ccname := os.Getenv("KRB5CCNAME")
		if strings.HasPrefix(ccname, "FILE:") {
			ccpath = strings.SplitN(ccname, ":", 2)[1]
		}

		ccache, err := credentials.LoadCCache(ccpath)
		if err != nil {
			return err
		}

		cl, err = client.NewFromCCache(ccache, cfg, client.DisablePAFXFAST(true))
		if err != nil {
			return err
		}
	}

	cl.Login()

	g.cli = cl

	return nil
}

// GetInitToken implements the GSS interface.
func (g *GSS) GetInitToken(host string, service string) ([]byte, error) {

	// Resolve the hostname down to an 'A' record, if required (usually, it is)
	if g.cli.Config.LibDefaults.DNSCanonicalizeHostname {
		var err error
		host, err = canonicalizeHostname(host)
		if err != nil {
			return nil, err
		}
	}

	spn := service + "/" + host

	return g.GetInitTokenFromSPN(spn)
}

// GetInitTokenFromSPN implements the GSS interface.
func (g *GSS) GetInitTokenFromSPN(spn string) ([]byte, error) {
	s := spnego.SPNEGOClient(g.cli, spn)

	st, err := s.InitSecContext()
	if err != nil {
		return nil, fmt.Errorf("kerberos error (InitSecContext): %s", err.Error())
	}

	b, err := st.Marshal()
	if err != nil {
		return nil, fmt.Errorf("kerberos error (Marshaling token): %s", err.Error())
	}

	return b, nil
}

// GetInitTokenFromSpn implements the GSS interface.
func (g *GSS) GetInitTokenFromSpn(spn string) ([]byte, error) {
	return g.GetInitTokenFromSPN(spn)
}

// Continue implements the GSS interface.
func (g *GSS) Continue(inToken []byte) (done bool, outToken []byte, err error) {
	t := &spnego.SPNEGOToken{}
	err = t.Unmarshal(inToken)
	if err != nil {
		return true, nil, fmt.Errorf("kerberos error (Unmarshaling token): %s", err.Error())
	}

	state := t.NegTokenResp.State()
	if state != spnego.NegStateAcceptCompleted {
		return true, nil, fmt.Errorf("kerberos: expected state 'Completed' - got %d", state)
	}

	return true, nil, nil
}

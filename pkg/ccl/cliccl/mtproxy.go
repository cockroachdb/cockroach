// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"crypto/tls"
	"io/ioutil"
	"net"
	"strings"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var sqlProxyListenAddr, sqlProxyTargetAddr string
var sqlProxyListenCert, sqlProxyListenKey string

func init() {
	startSQLProxyCmd := &cobra.Command{
		Use:   "start-proxy <basepath>",
		Short: "start-proxy host:port",
		Long: `Starts a SQL proxy for testing purposes.

This proxy provides very limited functionality. It accepts incoming connections
and relays them to the specified backend server after verifying that at least
one of the following holds:

1. the supplied database name is prefixed with 'prancing-pony.'; this prefix
   will then be removed for the connection to the backend server, and/or
2. the options parameter is 'prancing-pony'.

Connections to the target address use TLS but do not identify the identity of
the peer, making them susceptible to MITM attacks.
`,
		RunE: cli.MaybeDecorateGRPCError(runStartSQLProxy),
		Args: cobra.NoArgs,
	}
	f := startSQLProxyCmd.Flags()
	f.StringVarP(&sqlProxyListenCert, "listen-cert", "", "", "Certificate file to use for listener (auto-generate if empty)")
	f.StringVarP(&sqlProxyListenKey, "listen-key", "", "", "Private key file to use for listener(auto-generate if empty)")
	f.StringVarP(&sqlProxyListenAddr, "listen-addr", "", "127.0.0.1:46257", "Address for incoming connections")
	f.StringVarP(&sqlProxyTargetAddr, "target-addr", "", "127.0.0.1:26257", "Address for outgoing connections")
	cli.AddMTCommand(startSQLProxyCmd)
}

func runStartSQLProxy(*cobra.Command, []string) error {
	// openssl genrsa -out testserver.key 2048
	// openssl req -new -x509 -sha256 -key testserver.key -out testserver.crt -days 3650
	// ^-- Enter * as Common Name below, rest can be empty.
	certBytes := []byte(`-----BEGIN CERTIFICATE-----
MIICpDCCAYwCCQDWdkou+YTT/DANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
b2NhbGhvc3QwHhcNMjAwNTIwMTQxMjIyWhcNMzAwNTE4MTQxMjIyWjAUMRIwEAYD
VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDG
H6V5TZjppRR61azexRtKLOnftVpO0+CPslHynbWwrJ6sxZIdglUWoCT3a/93tq0h
SaMWIxH+29wDXiICCTbr485h3Sov5Rq7kV/AwcLMOpdqjbN2PRBW95aq8rV3h/Ui
K3hu8OZjeh4DzhxsDWYwLG+1aUHnzpwDVIvXqiiKHVtT3WLDHRNUAuph9o/4Fao0
m1KAzXvfbnNyMUWTAOUCIX2tlq79rEIAKOySCKDr07TuVrzCKcF5sbXkFXlmyFNl
KbmXRuD3UxghxLMmUZar7eZR84x6R/Rj5Dqyrs3nfl+/30Zk0pNe6naaKO39zqlR
rWQIqwSZrY1HwGGeVJFjAgMBAAEwDQYJKoZIhvcNAQELBQADggEBACluo7vP0kXd
uXD3joPKiMJ0FgZqeDtuSvBPfl0okqPN+bk/Huqu+FgxfChCs+2EcreGFxshjzuv
J58ogFq1YMB4pS4GlqarHE+UdliOobD+OyvX40w9lTJ2wI+v7kI79udFE+tyLIs6
YkuzFd1nB0Zcf8QFzyPRTVXVpsWid3ZvARDakp4z7klPLnkfVrXo/ivlKqGF+Ymy
vJ/riLR01omTVi6W40cml/H4DAtG/XVsQeFXWpjUv97MWGRVYycmpCleVkK+uC2x
XAEi/UMoPhhJd6HEWG+56IkFFoN4lNtPuyal0vzOJCn70pgQx3yKh61RQcPrJMlD
m9qz1xbrzj8=
-----END CERTIFICATE-----
`)
	keyBytes := []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAxh+leU2Y6aUUetWs3sUbSizp37VaTtPgj7JR8p21sKyerMWS
HYJVFqAk92v/d7atIUmjFiMR/tvcA14iAgk26+POYd0qL+Uau5FfwMHCzDqXao2z
dj0QVveWqvK1d4f1Iit4bvDmY3oeA84cbA1mMCxvtWlB586cA1SL16ooih1bU91i
wx0TVALqYfaP+BWqNJtSgM17325zcjFFkwDlAiF9rZau/axCACjskgig69O07la8
winBebG15BV5ZshTZSm5l0bg91MYIcSzJlGWq+3mUfOMekf0Y+Q6sq7N535fv99G
ZNKTXup2mijt/c6pUa1kCKsEma2NR8BhnlSRYwIDAQABAoIBADFpiSKUyNNU2aO9
EO1KaYD5bKbfmxNX4oTUK33/+WWD19stN0Dm1YPcEvwmUkOwKsPHksYdnwpaGSg5
3O93DtyMJ1ffCfuB/0XSfvgbGxNGdacciiquFhoqi8g82idioC+SeenpaPxcY4n9
aLdGLDtNidrL0qUWsXBfMLVr+cpgENPMiri31CGLNpfO1b4icdQjiltEn70To2Al
68Ptar60m/lJzf8QMFSf499/W3b7fLjGFK+Gzump94xAVMd7HhACf42ZpWRPe1Ih
lyHP6D0091cIRhGxIZrhLToSuySpf1A+C/rQYTqzEPv/a3b4Ja6poulpBppwJyDa
roC4KtkCgYEA/h0HRzekvNAg2cOV/Ag1RyE4AmdyCDubBHcPNJi9kI/RsA0RgurO
pr2oET0HWTENgE4e4hYQnlqUvTXYisvtvhigiCkcynpGoMJa5Y4St7J1QKdQtuwY
vcRqOGGSKl73biK79+BIV/6swWCkB+VzoGzKP8dY/XZHsI0FDdnia8UCgYEAx5gz
9qfzfiqOQP/GN6vGIzoldGxCDCHyyEZmvy0FiBlMJK36Qkjtz48eqYEXOUCX8Z5X
gB583iMv72Oz/wmefoIjnUd9uXyMqvhnYxG4vQhU4a83K4q4TPkNd7+sLiNqxIq2
o2jT6BktOHE5OiICeFGMFOfHtsyV78JMsuzUEwcCgYAXU5LXdsQokPJzCwE5oYdC
gEoj7lsJZm9UeZlrupmsK4eUIZ755ZQSulYzPubtyRL0NDehiWT9JFODCu5Vz2KD
kL8rwJpj+9V/7Fdrux78veUFilZedE3RHbaidlJ0kUMlWQroNi5t5XL2TWjBUM7M
azAlqqcAnVr3WfqcyuN+AQKBgQCsz+xV6I7bMy9dudc+hlyUTZj2V3FMHeyeWM5H
QkzizLxvma7vy0MUDd/HdTzNVk74ZVdvV3ZXwvGS/Klw7TwsXrNFTwvdGKiWs2KY
lVR1XwxXJyTGb2IpSw3NG8iRXhroNw3xKCcpcvsDPo0E90NaN4jo5NG3RSWgpINR
+9mW6wKBgCze3gZB6AU0W/Fluql88ANx/+nqZhrsJyfrv87AkgDtt0o1tOj+emKR
Uuwb2FVdh76ZK0AVd3Jh3KJs4+hr2u9syHaa7UPKXTcZsFWlGwZuu6X5A+0SO0S2
/ur8gv24YZJvV7OvPhw1SAuYL7MKMsfTW4TEKWTfkZWvm4YfZNmR
-----END RSA PRIVATE KEY-----
`)

	if (sqlProxyListenKey == "") != (sqlProxyListenCert == "") {
		return errors.New("must specify either both or neither of cert and key")
	}

	if sqlProxyListenCert != "" {
		var err error
		certBytes, err = ioutil.ReadFile(sqlProxyListenCert)
		if err != nil {
			return err
		}
	}
	if sqlProxyListenKey != "" {
		var err error
		keyBytes, err = ioutil.ReadFile(sqlProxyListenKey)
		if err != nil {
			return err
		}
	}

	cer, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", sqlProxyListenAddr)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()

	// Multiplex the listen address to give easy access to metrics from this
	// command.
	mux := cmux.New(ln)
	httpLn := mux.Match(cmux.HTTP1Fast())
	proxyLn := mux.Match(cmux.Any())

	outgoingConf := &tls.Config{
		InsecureSkipVerify: true,
	}
	server := sqlproxyccl.NewServer(sqlproxyccl.Options{
		IncomingTLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cer},
		},
		BackendFromParams: func(params map[string]string) (addr string, conf *tls.Config, clientErr error) {
			const magic = "prancing-pony"
			if strings.HasPrefix(params["database"], magic+".") {
				params["database"] = params["database"][len(magic)+1:]
				return sqlProxyTargetAddr, outgoingConf, nil
			}
			if params["options"] == "--cluster="+magic {
				return sqlProxyTargetAddr, outgoingConf, nil
			}
			return "", nil, errors.Errorf("client failed to pass '%s' via database or options", magic)
		},
	})

	group, ctx := errgroup.WithContext(context.Background())

	group.Go(func() error {
		return server.ServeHTTP(ctx, httpLn)
	})

	group.Go(func() error {
		return server.Serve(proxyLn)
	})

	group.Go(func() error {
		return mux.Serve()
	})

	return group.Wait()
}

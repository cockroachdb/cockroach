// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/url"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

const (
	// SslInlineURLParam is a non-standard connection URL
	// parameter. When true, we assume that sslcert, sslkey, and
	// sslrootcert contain URL-encoded data rather than paths.
	SslInlineURLParam = "sslinline"

	sslModeURLParam     = "sslmode"
	sslCertURLParam     = "sslcert"
	sslKeyURLParam      = "sslkey"
	sslRootCertURLParam = "sslrootcert"
)

func newPGConnForClient(
	ctx context.Context, remote url.URL, options *options,
) (*pgx.Conn, *pgx.ConnConfig, error) {
	config, err := setupPGXConfig(remote, options)
	if err != nil {
		return nil, nil, err
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, nil, err
	}
	return conn, config, nil
}

func setupPGXConfig(remote url.URL, options *options) (*pgx.ConnConfig, error) {
	noInlineCertURI, tlsInfo, err := uriWithInlineTLSCertsRemoved(remote)
	if err != nil {
		return nil, err
	}
	config, err := pgx.ParseConfig(noInlineCertURI.String())
	if err != nil {
		return nil, err
	}
	tlsInfo.addTLSCertsToConfig(config.TLSConfig)

	// The default pgx dialer uses a KeepAlive of 5 minutes. Set a lower KeepAlive
	// threshold, so if two nodes disconnect, we eagerly replan the job with
	// potentially new node pairings.
	dialer := &net.Dialer{KeepAlive: time.Second * 15}
	config.DialFunc = dialer.DialContext

	// If the user hasn't given us an application name.
	if _, ok := config.RuntimeParams["application_name"]; !ok {
		config.RuntimeParams["application_name"] = options.appName()
	}

	return config, nil
}

type tlsCerts struct {
	certs        []tls.Certificate
	rootCertPool *x509.CertPool
}

// uriWithInlineTLSCertsRemoved handles the non-standard sslinline
// option. The returned URL can be passed to pgx. The returned
// tlsCerts struct can be used to apply the certificate data to the
// tls.Config produced by pgx.
func uriWithInlineTLSCertsRemoved(remote url.URL) (url.URL, *tlsCerts, error) {
	if remote.Query().Get(SslInlineURLParam) != "true" {
		return remote, nil, nil
	}

	retURL := remote
	v := retURL.Query()
	cert := v.Get(sslCertURLParam)
	key := v.Get(sslKeyURLParam)
	rootcert := v.Get(sslRootCertURLParam)

	if (cert != "" && key == "") || (cert == "" && key != "") {
		return url.URL{}, nil, errors.New(`both "sslcert" and "sslkey" are required`)
	}

	tlsInfo := &tlsCerts{}
	if rootcert != "" {
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM([]byte(rootcert)) {
			return url.URL{}, nil, errors.New("unable to add CA to cert pool")
		}
		tlsInfo.rootCertPool = caCertPool
	}
	if cert != "" && key != "" {
		// TODO(ssd): pgx supports sslpassword here. But, it
		// only supports PKCS#1 and relies on functions that
		// are deprecated in the stdlib. For now, I've skipped
		// it.
		block, _ := pem.Decode([]byte(key))
		pemKey := pem.EncodeToMemory(block)
		keyPair, err := tls.X509KeyPair([]byte(cert), pemKey)
		if err != nil {
			return url.URL{}, nil, errors.Wrap(err, "unable to construct x509 key pair")
		}
		tlsInfo.certs = []tls.Certificate{keyPair}
	}

	// lib/pq, pgx, and the C libpq implement this backwards
	// compatibility quirk. Since we are removing sslrootcert, we
	// have to re-implement it here.
	//
	// TODO(ssd): This may be a sign that we should implement the
	// entire configTLS function from pgx and remove all tls
	// options.
	if v.Get(sslModeURLParam) == "require" && rootcert != "" {
		v.Set(sslModeURLParam, "verify-ca")
	}

	v.Del(sslCertURLParam)
	v.Del(sslKeyURLParam)
	v.Del(sslRootCertURLParam)
	v.Del(SslInlineURLParam)
	retURL.RawQuery = v.Encode()
	return retURL, tlsInfo, nil
}

func (c *tlsCerts) addTLSCertsToConfig(tlsConfig *tls.Config) {
	if c == nil {
		return
	}

	if c.rootCertPool != nil {
		tlsConfig.RootCAs = c.rootCertPool
		tlsConfig.ClientCAs = c.rootCertPool
	}

	if len(c.certs) > 0 {
		tlsConfig.Certificates = c.certs
	}
}

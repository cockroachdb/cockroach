// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const certLifetime = 30 * 24 * time.Hour

func strToBool(src string, dest *bool) (wasSet bool, err error) {
	b, err := strconv.ParseBool(src)
	if err != nil {
		return false, err
	}
	*dest = b
	return true, nil
}

func decodeBase64FromString(src string, dest *[]byte) error {
	if src == `` {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(src)
	if err != nil {
		return err
	}
	*dest = decoded
	return nil
}

func encodeBase64ToString(src []byte, dest *string) {
	if src != nil {
		encoded := base64.StdEncoding.EncodeToString(src)
		*dest = encoded
	}
}

func newClientFromTLSKeyPair(caCert []byte) (*httputil.Client, error) {
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.Wrap(err, "could not load system root CA pool")
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	if caCert != nil {
		if !rootCAs.AppendCertsFromPEM(caCert) {
			return nil, errors.Errorf("failed to parse certificate data:%s", string(caCert))
		}
	}

	client := &httputil.Client{
		Client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: rootCAs,
				},
			},
		},
	}
	return client, nil
}

func generateCACert(priv *rsa.PrivateKey) ([]byte, *x509.Certificate, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}

	certSpec := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Country:            []string{"US"},
			Organization:       []string{"Cockroach Labs"},
			OrganizationalUnit: []string{"Engineering"},
			CommonName:         "Roachtest Temporary Insecure CA",
		},
		NotBefore:             timeutil.Now(),
		NotAfter:              timeutil.Now().Add(certLifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	cert, err := x509.CreateCertificate(rand.Reader, certSpec, certSpec, &priv.PublicKey, priv)
	return cert, certSpec, err
}

func pemEncode(dataType string, data []byte) (string, error) {
	ret := new(strings.Builder)
	err := pem.Encode(ret, &pem.Block{Type: dataType, Bytes: data})
	if err != nil {
		return "", err
	}

	return ret.String(), nil
}

func pemEncodePrivateKey(key *rsa.PrivateKey) (string, error) {
	return pemEncode("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
}

func pemEncodeCert(cert []byte) (string, error) {
	return pemEncode("CERTIFICATE", cert)
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	ret, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, errors.Wrap(err, "generate random serial")
	}
	return ret, nil
}

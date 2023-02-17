// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
)

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

func newClientFromTLSKeyPair(caCert, clientCert, clientKey []byte) (*httputil.Client, error) {
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.Wrap(err, "could not load system root CA pool")
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	if len(caCert) > 0 && !rootCAs.AppendCertsFromPEM(caCert) {
		return nil, errors.Errorf("failed to parse certificate data:%s", string(caCert))
	}

	tlsConfig := &tls.Config{
		RootCAs: rootCAs,
	}

	clientCertProvided := len(clientCert) != 0
	clientKeyProvided := len(clientKey) != 0
	if clientCertProvided != clientKeyProvided {
		return nil, errors.Errorf("%s and %s must be provided together",
			changefeedbase.RegistryParamClientCert, changefeedbase.RegistryParamClientKey)
	}

	client := httputil.NewClientWithTimeout(httputil.StandardHTTPTimeout)
	transport := client.Transport.(*http.Transport)

	if clientCertProvided {
		cert, err := tls.X509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, errors.Wrap(err, `invalid client certificate data provided`)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	transport.TLSClientConfig = tlsConfig
	client.Client.Transport = transport

	return client, nil
}

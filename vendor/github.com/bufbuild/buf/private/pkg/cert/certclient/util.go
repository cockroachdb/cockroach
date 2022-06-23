// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package certclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// newClientTLSConfigFromRootCertFiles creates a new tls.Config from a root certificate files.
func newClientTLSConfigFromRootCertFiles(rootCertFilePaths ...string) (*tls.Config, error) {
	rootCertDatas := make([][]byte, len(rootCertFilePaths))
	for i, rootCertFilePath := range rootCertFilePaths {
		rootCertData, err := os.ReadFile(rootCertFilePath)
		if err != nil {
			return nil, err
		}
		rootCertDatas[i] = rootCertData
	}
	return newClientTLSConfigFromRootCertDatas(rootCertDatas...)
}

// newClientTLSConfigFromRootCertDatas creates a new tls.Config from root certificate datas.
func newClientTLSConfigFromRootCertDatas(rootCertDatas ...[]byte) (*tls.Config, error) {
	certPool := x509.NewCertPool()
	for _, rootCertData := range rootCertDatas {
		if !certPool.AppendCertsFromPEM(rootCertData) {
			return nil, errors.New("failed to append root certificate")
		}
	}
	return newClientTLSConfigFromRootCertPool(certPool), nil
}

// newClientTLSConfigFromRootCertPool creates a new tls.Config from a root certificate pool.
func newClientTLSConfigFromRootCertPool(certPool *x509.CertPool) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    certPool,
	}
}

// newClientSystemTLSConfig creates a new tls.Config that uses the system cert pool for verifying
// server certificates.
func newClientSystemTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// An empty TLS config will use the system certificate pool
		// when verifying the servers certificate. This is because
		// not setting any RootCAs will set `x509.VerifyOptions.Roots`
		// to nil, which triggers the loading of system certs (including
		// on Windows somehow) within (*x509.Certificate).Verify.
		RootCAs: nil,
	}
}

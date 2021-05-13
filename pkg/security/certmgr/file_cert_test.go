// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package certmgr

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateCert(t *testing.T, certFile, keyFile *os.File, startTime time.Time) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    startTime,
		NotAfter:     startTime.AddDate(10, 0, 0),
		DNSNames:     []string{"localhost"},
		IsCA:         true,
	}
	require.NoError(t, certFile.Truncate(0))
	_, err = certFile.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, keyFile.Truncate(0))
	_, err = keyFile.Seek(0, 0)
	require.NoError(t, err)
	template.NotBefore = startTime
	cer, err := x509.CreateCertificate(rand.Reader, &template, &template, pub, priv)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: cer}))
	key, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: key}))
}

func TestFileCert_Err(t *testing.T) {
	fc := NewFileCert("abc", "def")
	require.NotNil(t, fc)
	require.Nil(t, fc.Err())
	fc.Reload(context.Background())
	require.NotNil(t, fc.Err())
	fc.ClearErr()
	require.Nil(t, fc.Err())
}

func TestFileCert_Reload(t *testing.T) {
	// 1. Reload cert when cert or key don't exist.
	fc1 := NewFileCert("abc", "def")
	require.NotNil(t, fc1)
	fc1.Reload(context.Background())
	require.Regexp(t, "open abc: no such file or directory", fc1.Err())

	// 2. Reload cert when the cert and/or key are invalid (empty)
	certFile, err := ioutil.TempFile("", "test*.crt")
	require.NoError(t, err)
	defer func() { assert.NoError(t, os.Remove(certFile.Name())) }()
	keyFile, err := ioutil.TempFile("", "test*.key")
	require.NoError(t, err)
	defer func() { assert.NoError(t, os.Remove(keyFile.Name())) }()
	fc2 := NewFileCert(certFile.Name(), keyFile.Name())
	require.NotNil(t, fc2)
	fc2.Reload(context.Background())
	require.Regexp(t, "tls: failed to find any PEM data in certificate input", fc2.Err())

	// 3. Reload cert when the cert and key are valid.
	fc2.ClearErr()
	now := timeutil.Now()
	generateCert(t, certFile, keyFile, now)
	fc2.Reload(context.Background())
	require.NoError(t, fc2.Err())
	require.NotNil(t, fc2.TLSCert())
	require.Len(t, fc2.TLSCert().Certificate, 1)
	parsedCert, err := x509.ParseCertificate(fc2.TLSCert().Certificate[0])
	require.NoError(t, err)
	require.Equal(t, now.Unix(), parsedCert.NotBefore.Unix())

	// 4. Reload cert when the cert and the key are updated.
	oneYearFromNow := now.AddDate(1, 0, 0)
	generateCert(t, certFile, keyFile, oneYearFromNow)
	fc2.Reload(context.Background())
	require.NoError(t, fc2.Err())
	require.NotNil(t, fc2.TLSCert())
	require.Len(t, fc2.TLSCert().Certificate, 1)
	parsedCert, err = x509.ParseCertificate(fc2.TLSCert().Certificate[0])
	require.NoError(t, err)
	require.Equal(t, oneYearFromNow.Unix(), parsedCert.NotBefore.Unix())
}

func TestMakeFileCert(t *testing.T) {
	type args struct {
		certFile string
		keyFile  string
	}
	tests := []struct {
		name string
		args args
		want *FileCert
	}{
		{
			"test",
			args{certFile: "abc", keyFile: "def"},
			&FileCert{certFile: "abc", keyFile: "def", err: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewFileCert(tt.args.certFile, tt.args.keyFile)
			assert.Equal(t, tt.want.certFile, got.certFile)
			assert.Equal(t, tt.want.keyFile, got.keyFile)
			assert.Nil(t, got.cert)
			assert.Nil(t, got.err)
		})
	}
}

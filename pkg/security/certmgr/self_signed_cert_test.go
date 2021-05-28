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
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSelfSignedCert_Err(t *testing.T) {
	ssc := NewSelfSignedCert(-9999, 0, 0, 0)
	require.NotNil(t, ssc)
	require.Nil(t, ssc.Err())
	ssc.Reload(context.Background())
	require.Regexp(t, "cannot represent time as GeneralizedTime", ssc.Err())
	ssc.ClearErr()
	require.Nil(t, ssc.Err())
}

func TestSelfSignedCert_TLSCert(t *testing.T) {
	ssc := NewSelfSignedCert(1, 6, 3, 5*time.Hour)
	require.NotNil(t, ssc)
	require.Nil(t, ssc.Err())
	ssc.Reload(context.Background())
	require.Nil(t, ssc.Err())
	require.NotNil(t, ssc.TLSCert())
	require.Len(t, ssc.TLSCert().Certificate, 1)
	cert, err := x509.ParseCertificate(ssc.TLSCert().Certificate[0])
	require.NoError(t, err)
	expectedUntil := cert.NotBefore.AddDate(1, 6, 3).Add(5 * time.Hour)
	require.Equal(t, expectedUntil, cert.NotAfter)
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"io/ioutil"
	"math/rand"
	"path"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestJoinToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	j := &joinToken{
		tokenID:      uuid.MakeV4(),
		sharedSecret: randutil.RandBytes(rng, joinTokenSecretLen),
		fingerprint:  nil,
	}
	testCACert := []byte("foobar")
	j.sign(testCACert)
	require.True(t, j.verifySignature(testCACert))
	require.False(t, j.verifySignature([]byte("test")))
	require.NotNil(t, j.fingerprint)

	marshaled, err := j.MarshalText()
	require.NoError(t, err)
	j2 := &joinToken{}
	require.NoError(t, j2.UnmarshalText(marshaled))

	require.Equal(t, j, j2)
}

func TestGenerateJoinToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	caCertFile := path.Join(tempDir, security.CACertFilename())
	caCert := []byte("foobar")
	require.NoError(t, ioutil.WriteFile(caCertFile, caCert, 0600))

	token, err := generateJoinToken(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.True(t, token.verifySignature(caCert))
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"math/rand"
	"testing"

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
	j := &JoinToken{
		TokenID:      uuid.MakeV4(),
		SharedSecret: randutil.RandBytes(rng, joinTokenSecretLen),
		fingerprint:  nil,
	}
	testCACert := []byte("foobar")
	j.sign(testCACert)
	require.True(t, j.VerifySignature(testCACert))
	require.False(t, j.VerifySignature([]byte("test")))
	require.NotNil(t, j.fingerprint)

	marshaled, err := j.MarshalText()
	require.NoError(t, err)
	j2 := &JoinToken{}
	require.NoError(t, j2.UnmarshalText(marshaled))

	require.Equal(t, j, j2)
}

func TestGenerateJoinToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cm, err := NewCertificateManager(EmbeddedCertsDir, CommandTLSSettings{})
	require.NoError(t, err)

	token, err := GenerateJoinToken(cm)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.True(t, token.VerifySignature(cm.CACert().FileContents))
}

func TestJoinTokenVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cm, err := NewCertificateManager(EmbeddedCertsDir, CommandTLSSettings{})
	require.NoError(t, err)

	token, err := GenerateJoinToken(cm)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.True(t, token.VerifySignature(cm.CACert().FileContents))

	t.Run("supported", func(t *testing.T) {
		// No error when (un)marshaling with supported version.
		b, err := token.MarshalText()
		require.NoError(t, err)
		token1 := new(JoinToken)
		err = token1.UnmarshalText(b)
		require.NoError(t, err)
	})

	t.Run("unsupported_unmarshal", func(t *testing.T) {
		b, err := token.MarshalText()
		require.NoError(t, err)
		// Set first byte to unsupported version.
		b[0] = 'x'
		// Expect unmarshal to fail with token version error.
		var j2 JoinToken
		err = j2.UnmarshalText(b)
		require.Error(t, err)
		require.EqualValues(t, err, errInvalidJoinToken)
	})
}

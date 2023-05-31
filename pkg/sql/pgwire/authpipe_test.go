// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func createAuthPipe() *authPipe {
	return newAuthPipe(
		nil,   /* c */
		false, /* logAuthn */
		authOptions{
			connType: hba.ConnLocal,
		},
		username.SQLUsername{},
	)
}

func TestNoMorePwdDataIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ap := createAuthPipe()
	ap.noMorePwdData()
	ap.noMorePwdData()
}

func TestGetPwdDataAfterNoMorePwdData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ap := createAuthPipe()
	ap.noMorePwdData()
	// GetPwdData should not hang waiting for p.writerDone to close.
	_, err := ap.GetPwdData()
	require.ErrorContains(t, err, writerDoneError)
}

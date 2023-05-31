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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNoMorePwdDataGetPwdData(t *testing.T) {
	ap := newAuthPipe(
		nil,   /* c */
		false, /* logAuthn */
		authOptions{
			connType: hba.ConnLocal,
		},
		username.SQLUsername{},
	)
	ap.noMorePwdData()
	// GetPwdData should not hang waiting for p.writerDone to close.
	_, err := ap.GetPwdData()
	require.ErrorContains(t, err, writerDoneError)
}

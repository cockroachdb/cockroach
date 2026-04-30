// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssh

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/assert"
)

func TestIsPermanentSSHError_AuthFailure(t *testing.T) {
	tests := []struct {
		msg       string
		permanent bool
	}{
		{"unable to authenticate, attempted methods [none publickey]", true},
		{"ssh: handshake failed: ssh: unable to authenticate", true},
		{"no supported methods remain", true},
		{"dial tcp 1.2.3.4:22: connection refused", false},
		{"dial tcp 1.2.3.4:22: i/o timeout", false},
		{"read tcp: read: connection reset by peer", false},
		// Handshake timeout is transient and should be retried.
		{"ssh: handshake failed: read tcp 1.2.3.4:22: i/o timeout", false},
	}
	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			err := &testError{msg: tt.msg}
			got := isPermanentSSHError(err)
			assert.Equal(t, tt.permanent, got)
		})
	}
}

func TestBackoffDurations(t *testing.T) {
	assert.Len(t, backoffDurations, maxConnRetries)
}

func TestSSHClient_InvalidKey(t *testing.T) {
	// This test doesn't need a real server — key parsing fails before
	// any network call.
	client := NewSSHClient()
	_, _, err := client.RunCommand(
		context.Background(), logger.DefaultLogger, "127.0.0.1:22", "test",
		[]byte("not a key"), "echo hi",
	)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "parse SSH private key"))
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

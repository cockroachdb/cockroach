// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
)

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

func TestVerifyClusterName(t *testing.T) {
	findActiveAccounts = func(l *logger.Logger) (map[string]string, error) {
		return map[string]string{"1": "user1", "2": "user2", "3": "USER4"}, nil
	}
	defer func() {
		findActiveAccounts = vm.FindActiveAccounts
	}()
	cases := []struct {
		description, clusterName, username string
		errorExpected                      bool
	}{
		{
			"username found", "user1-clustername", "", false,
		},
		{
			"username not found", "user3-clustername", "", true,
		},
		{
			"specified username", "user3-clustername", "user3", false,
		},
		{
			"specified username that doesn't match", "user1-clustername", "fakeuser", true,
		},
		{
			"clustername not sanitized", "UserName-clustername", "", true,
		},
		{
			"no username", "clustername", "", true,
		},
		{
			"no clustername", "user1", "", true,
		},
		{
			"unsanitized found username", "user4-clustername", "", false,
		},
		{
			"unsanitized specified username", "user3-clustername", "USER3", false,
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			if c.errorExpected {
				assert.Error(t, verifyClusterName(nilLogger(), c.clusterName, c.username))
			} else {
				assert.NoError(t, verifyClusterName(nilLogger(), c.clusterName, c.username))
			}
		})
	}
}

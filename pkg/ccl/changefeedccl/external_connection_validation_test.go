// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestValidateExternalConnectionSinkURI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, stopServer := makeServer(t)
	defer stopServer()

	env := externalconn.ExternalConnEnv{
		ServerCfg: &s.DistSQLServer().(*distsql.ServerImpl).ServerConfig,
		Username:  username.RootUserName(),
	}

	t.Run("fails-when-dial-fails", func(t *testing.T) {
		// Using a port that is unlikely to be listening.
		uri := "kafka://127.0.0.1:1"
		err := validateExternalConnectionSinkURI(ctx, env, uri)
		if err == nil {
			t.Fatal("expected error when dialing invalid sink, got nil")
		}
		// Verify it's a connection error, not a validation error.
		expectedErr := "dial"
		if !testutils.IsError(err, expectedErr) && !testutils.IsError(err, "connection") && !testutils.IsError(err, "broker") {
			t.Fatalf("expected connection error, got: %v", err)
		}
		fmt.Printf("Successfully caught expected error: %v\n", err)
	})

	t.Run("succeeds-when-well-formed-null-sink", func(t *testing.T) {
		// Null sink dial should always succeed.
		uri := "null:///"
		err := validateExternalConnectionSinkURI(ctx, env, uri)
		if err != nil {
			t.Fatalf("expected success for null sink, got: %v", err)
		}
	})
}

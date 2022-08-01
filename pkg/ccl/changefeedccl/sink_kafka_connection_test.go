// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestChangefeedExternalConnections(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{{
				Key:   "region",
				Value: testServerRegion,
			}},
		},
	})

	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	enableEnterprise := utilccl.TestingDisableEnterprise()
	enableEnterprise()
	unknownParams := func(sink string, params ...string) string {
		return fmt.Sprintf(`unknown %s sink query parameters: [%s]`, sink, strings.Join(params, ", "))
	}

	for _, tc := range []struct {
		name          string
		uri           string
		expectedError string
	}{
		{
			// kafka_topic_prefix was referenced by an old version of the RFC, it's
			// "topic_prefix" now.
			name:          "kafka_topic_prefix",
			uri:           "kafka://nope/?kafka_topic_prefix=foo",
			expectedError: unknownParams(`kafka`, `kafka_topic_prefix`),
		},
		{
			// schema_topic will be implemented but isn't yet.
			name:          "schema_topic is not yet supported",
			uri:           "kafka://nope/?schema_topic=foo",
			expectedError: "schema_topic is not yet supported",
		},
		// Sanity check kafka tls parameters.
		{
			name:          "param tls_enabled must be a bool",
			uri:           "kafka://nope/?tls_enabled=foo",
			expectedError: "param tls_enabled must be a bool",
		},
		{
			name:          "param insecure_tls_skip_verify must be a bool",
			uri:           "kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=foo",
			expectedError: "param insecure_tls_skip_verify must be a bool",
		},
		{
			name:          "param ca_cert must be base 64 encoded",
			uri:           "kafka://nope/?ca_cert=!",
			expectedError: "param ca_cert must be base 64 encoded",
		},
		{
			name:          "ca_cert requires tls_enabled=true",
			uri:           "kafka://nope/?&ca_cert=Zm9v",
			expectedError: "ca_cert requires tls_enabled=true",
		},
		{
			name:          "param client_cert must be base 64 encoded",
			uri:           "kafka://nope/?client_cert=!",
			expectedError: "param client_cert must be base 64 encoded",
		},
		{
			name:          "param client_key must be base 64 encoded",
			uri:           "kafka://nope/?client_key=!",
			expectedError: "param client_key must be base 64 encoded",
		},
		{
			name:          "client_cert requires tls_enabled=true",
			uri:           "kafka://nope/?client_cert=Zm9v",
			expectedError: "client_cert requires tls_enabled=true",
		},
		{
			name:          "client_cert requires client_key to be set",
			uri:           "kafka://nope/?tls_enabled=true&client_cert=Zm9v",
			expectedError: "client_cert requires client_key to be set",
		},
		{
			name:          "client_key requires client_cert to be set",
			uri:           "kafka://nope/?tls_enabled=true&client_key=Zm9v",
			expectedError: "client_key requires client_cert to be set",
		},
		{
			name:          "invalid client certificate",
			uri:           "kafka://nope/?tls_enabled=true&client_cert=Zm9v&client_key=Zm9v",
			expectedError: "invalid client certificate",
		},
		// Sanity check kafka sasl parameters.
		{
			name:          "param sasl_enabled must be a bool",
			uri:           "kafka://nope/?sasl_enabled=maybe",
			expectedError: "param sasl_enabled must be a bool",
		},
		{
			name:          "param sasl_handshake must be a bool",
			uri:           "kafka://nope/?sasl_enabled=true&sasl_handshake=maybe",
			expectedError: "param sasl_handshake must be a bool",
		},
		{
			name:          "sasl_enabled must be enabled to configure SASL handshake behavior",
			uri:           "kafka://nope/?sasl_handshake=false",
			expectedError: "sasl_enabled must be enabled to configure SASL handshake behavior",
		},
		{
			name:          "sasl_user must be provided when SASL is enabled",
			uri:           "kafka://nope/?sasl_enabled=true",
			expectedError: "sasl_user must be provided when SASL is enabled",
		},
		{
			name:          "sasl_password must be provided when SASL is enabled",
			uri:           "kafka://nope/?sasl_enabled=true&sasl_user=a",
			expectedError: "sasl_password must be provided when SASL is enabled",
		},
		{
			name:          "sasl_enabled must be enabled if a SASL user is provided",
			uri:           "kafka://nope/?sasl_user=a",
			expectedError: "sasl_enabled must be enabled if a SASL user is provided",
		},
		{
			name:          "sasl_enabled must be enabled if a SASL password is provided",
			uri:           "kafka://nope/?sasl_password=a",
			expectedError: "sasl_enabled must be enabled if a SASL password is provided",
		},
		{
			name:          "sasl_enabled must be enabled to configure SASL mechanism",
			uri:           "kafka://nope/?sasl_mechanism=SCRAM-SHA-256",
			expectedError: "sasl_enabled must be enabled to configure SASL mechanism",
		},
		{
			name:          "param sasl_mechanism must be one of SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN",
			uri:           "kafka://nope/?sasl_enabled=true&sasl_mechanism=unsuppported",
			expectedError: "param sasl_mechanism must be one of SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, tc.expectedError,
				fmt.Sprintf(`CREATE EXTERNAL CONNECTION '%s' AS '%s'`, tc.name, tc.uri),
			)
		})
	}

	t.Run("changefeed-with-well-formed-uri", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION nope AS 'kafka://nope'`)
		sqlDB.ExpectErr(t, `client has run out of available brokers`,
			`CREATE CHANGEFEED FOR foo INTO 'external://nope'`)

		sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION "nope-with-params" AS 'kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=true&topic_name=foo'`)
		sqlDB.ExpectErr(t, `client has run out of available brokers`,
			`CREATE CHANGEFEED FOR foo INTO 'external://nope-with-params'`)

		sqlDB.ExpectErr(
			t, `client has run out of available brokers`,
			`CREATE CHANGEFEED FOR foo INTO 'external://nope/' WITH kafka_sink_config='{"Flush": {"Messages": 100, "Frequency": "1s"}}'`,
		)

		sqlDB.ExpectErr(
			t, `this sink is incompatible with option webhook_client_timeout`,
			`CREATE CHANGEFEED FOR foo INTO 'external://nope/' WITH webhook_client_timeout='1s'`,
		)
	})
}

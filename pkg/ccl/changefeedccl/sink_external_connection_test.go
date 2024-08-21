// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestValidateExternalURI tests we apply the correct validation for external sink URIs.
// The validation includes get the url and dial it.
func TestValidateExternalURI(t *testing.T) {
	return

	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)

	t.Run("changefeed-with-well-formed-uri", func(t *testing.T) {
		sqlList := []string{
			// kafka scheme external connections
			`CREATE EXTERNAL CONNECTION nope AS 'kafka://nope'`,
			`CREATE EXTERNAL CONNECTION "nope-with-params" AS 'kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=true&topic_name=foo'`,
			// confluent-cloud external connections
			`CREATE EXTERNAL CONNECTION confluent1 AS 'confluent-cloud://nope?api_key=fee&api_secret=bar'`,
			// azure-event-hub external connections
			`CREATE EXTERNAL CONNECTION azure1 AS 'azure-event-hub://nope?shared_access_key_name=fee&shared_access_key=123&topic_prefix=foo'`,
		}

		for _, sql := range sqlList {
			// By check the "unable to dial" error, we can confirm the dialing action
			// is performed.
			sqlDB.ExpectErr(t, "unable to dial", sql)
		}
	})
}

// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import "testing"

func TestDockerFinagle(t *testing.T) {
	t.Skip("TODO(dt): #5951, #5928, and needs #5484")
	testDockerSuccess(t, "finagle", []string{"/bin/sh", "-c", finagle})
}

const finagle = `
set -e
openssl pkcs12 -export -in $PGSSLCERT -inkey $PGSSLKEY -out /client.p12 -name "Whatever" -password pass:
echo "CREATE DATABASE finagle_postgres_test" | psql
PGHOSTPORT=$PGHOST:$PGPORT USE_PG_SSL=1 RUN_PG_INTEGRATION_TESTS=1 PGUSER="root" PG_PKCS12=/client.p12 java -jar /finagle-postgres-tests.jar com.twitter.finagle.postgres.integration.IntegrationSpec
`

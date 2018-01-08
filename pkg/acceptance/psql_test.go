// Copyright 2017 The Cockroach Authors.
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

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDockerPSQL(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "psql", []string{"/bin/bash", "-c", psql})
}

const psql = `
set -xeuo pipefail

# Check that psql works in the first place.
psql -c "select 1" | grep "1 row"

# Check that COPY works outside of a transaction (#13395)
psql -d testdb <<EOF
CREATE DATABASE IF NOT EXISTS testdb;
CREATE TABLE ints (a INTEGER NOT NULL);
CREATE TABLE playground (
    equip_id integer NOT NULL,
    type character varying(50) NOT NULL,
    color character varying(25) NOT NULL,
    location character varying(25),
    install_date date,
    ip inet
);

COPY playground (equip_id, type, color, location, install_date, ip) FROM stdin;
1	slide	blue	south	2014-04-28	192.168.0.1
2	swing	yellow	northwest	2010-08-16	ffff::ffff:12
\.
EOF
# psql does not report failures properly in its exit code, so we check
# that the value was inserted explicitly.
psql -d testdb -c "SELECT * FROM playground"  | grep blue
psql -d testdb -c "SELECT * FROM playground"  | grep ffff::ffff:12

# Test lack of newlines at EOF with no slash-dot.
echo 'COPY playground (equip_id, type, color, location, install_date, ip) FROM stdin;' > import.sql
echo -n -e '3\trope\tgreen\teast\t2015-01-02\t192.168.0.1' >> import.sql
psql -d testdb < import.sql
psql -d testdb -c "SELECT * FROM playground"  | grep green
psql -d testdb -c "SELECT * FROM playground"  | grep 192.168.0.1

# Test lack of newlines at EOF with slash-dot.
echo 'COPY playground (equip_id, type, color, location, install_date, ip) FROM stdin;' > import.sql
echo -e '4\tsand\tbrown\twest\t2016-03-04\t192.168.0.1' >> import.sql
echo -n '\.' >> import.sql
psql -d testdb < import.sql
psql -d testdb -c "SELECT * FROM playground"  | grep brown

# Test that the app name set in the pgwire init exchange is propagated
# down the session.
psql -d testdb -c "show application_name" | grep psql

# Test that errors in COPY FROM STDIN don't screw up the connection
# See #16393
echo 'COPY playground (equip_id, type, color, location, install_date, ip) FROM stdin;' > import.sql
echo -e '3\tjunk\tgreen\teast\t2015-01-02\t192.168.0.1' >> import.sql
echo 'garbage' >> import.sql
echo '\.' >> import.sql
echo "SELECT 'hooray'" >> import.sql
psql -d testdb < import.sql | grep hooray
# Assert the junk line wasn't added.
psql -d testdb -c "SELECT * from playground WHERE type='junk'" | grep "0 rows"

# Test that large COPY FROM STDIN commands don't create a bad connection status.
# See issue #17941.
echo 'COPY ints FROM stdin;' > import.sql
for i in {1..1000}; do
    echo $i >> import.sql
done
echo "\." >> import.sql
psql -d testdb < import.sql
psql -d testdb -c "SELECT COUNT(*) FROM ints" | grep "1000"

# Test that CREATE TABLE AS returns tag SELECT, not CREATE (#20227).
psql -d testdb -c "CREATE TABLE ctas AS SELECT 1" | grep "SELECT"

exit 0
`

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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package acceptance

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"
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
CREATE TABLE playground (
    equip_id integer NOT NULL,
    type character varying(50) NOT NULL,
    color character varying(25) NOT NULL,
    location character varying(25),
    install_date date
);

COPY playground (equip_id, type, color, location, install_date) FROM stdin;
1	slide	blue	south	2014-04-28
2	swing	yellow	northwest	2010-08-16
\.
EOF
# psql does not report failures properly in its exit code, so we check
# that the value was inserted explicitly.
psql -d testdb -c "SELECT * FROM playground"  | grep blue

# Test lack of newlines at EOF with no slash-dot.
echo 'COPY playground (equip_id, type, color, location, install_date) FROM stdin;' > import.sql
echo -n -e '3\trope\tgreen\teast\t2015-01-02' >> import.sql
psql -d testdb < import.sql
psql -d testdb -c "SELECT * FROM playground"  | grep green

# Test lack of newlines at EOF with slash-dot.
echo 'COPY playground (equip_id, type, color, location, install_date) FROM stdin;' > import.sql
echo -e '4\tsand\tbrown\twest\t2016-03-04' >> import.sql
echo -n '\.' >> import.sql
psql -d testdb < import.sql
psql -d testdb -c "SELECT * FROM playground"  | grep brown

# Test that the app name set in the pgwire init exchange is propagated
# down the session.
psql -d testdb -c "show application_name" | grep psql

exit 0
`

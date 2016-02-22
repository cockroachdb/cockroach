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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"strings"
	"testing"
)

func TestDockerJava(t *testing.T) {
	setupSys := strings.Replace(setupConn, "%D", "", 1)
	setupTest := strings.Replace(setupConn, "%D", "test", 1)

	test1 := strings.Replace(createdbTest, "%C", setupSys, 1)
	test1 = strings.Replace(test1, "%T", createdbTest, 1)
	testDockerSuccess(t, "java", []string{"/bin/sh", "-c", test1})

	test2 := strings.Replace(createdbTest, "%C", setupTest, 1)
	test2 = strings.Replace(test2, "%T", schemaTests, 1)
	testDockerSuccess(t, "java", []string{"/bin/sh", "-c", test2})

	test3 := strings.Replace(createdbTest, "%C", setupTest, 1)
	test3 = strings.Replace(test2, "%T", valueTests, 1)
	testDockerSuccess(t, "java", []string{"/bin/sh", "-c", strings.Replace(test3, "%v", "Int(2, 3)", 1)})
	testDockerFail(t, "java", []string{"/bin/sh", "-c", strings.Replace(test3, "%v", `String(2, "a")`, 1)})
}

const setupConn = `
		Class.forName("org.postgresql.Driver");

		String urlBase = "jdbc:postgresql://";
		urlBase += System.getenv("PGHOST") + ":" + System.getenv("PGPORT");
		urlBase += "/";

    String sslSettings = ?ssl=true";
		sslSettings += "&sslcert=" + System.getenv("PGSSLCERT");
		sslSettings += "&sslkey=key.pk8";
	  sslSettings += "&sslrootcert=/certs/ca.crt";
		sslSettings += "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory";

		Connection conn = DriverManager.getConnection(urlBase + "%D" + sslSettings);
`

const createdbTest = `
    PreparedStatement stmt = conn.prepareStatement("CREATE DATABASE test");
    int res = stmt.executeUpdate();
    if (res != 0) {
        throw new Exception("unexpected");
    }
`

const schemaTests = `
    PreparedStatement stmt = conn.prepareStatement("CREATE TABLE f (x INT)");
    int res = stmt.executeUpdate();
    if (res != 0) {
        throw new Exception("unexpected");
    }

    stmt = conn.prepareStatement("INSERT INTO f VALUES (42)");
    res = stmt.executeUpdate();
    if (res != 1) {
        throw new Exception("unexpected");
    }

		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM f");
		ResultSet rs = stmt.executeQuery();
		rs.next();
		int a = rs.getInt(1);
    if (a != 42) {
        throw new Exception("unexpected");
    } 

    stmt = conn.prepareStatement("DROP TABLE f");
    res = stmt.executeUpdate();
    if (res != 0) {
        throw new Exception("unexpected");
    }
`

const valueTests = `
		PreparedStatement stmt = conn.prepareStatement("SELECT 1, 2 > ?, ?::int, ?::string, ?::string, ?::string, ?::string, ?::string");
		stmt.setInt(1, 3);
		stmt.set%v;

		stmt.setBoolean(3, true);
		stmt.setLong(4, -4L);
		stmt.setFloat(5, 5.31f);
		stmt.setDouble(6, -6.21d);
		stmt.setShort(7, (short)7);

		ResultSet rs = stmt.executeQuery();
		rs.next();
		int a = rs.getInt(1);
		boolean b = rs.getBoolean(2);
		int c = rs.getInt(3);
		String d = rs.getString(4);
		String e = rs.getString(5);
		String f = rs.getString(6);
		String g = rs.getString(7);
		String h = rs.getString(8);
		if (a != 1 || b != false || c != 3 || !d.equals("true") || !e.equals("-4") || !f.startsWith("5.3") || !g.startsWith("-6.2") || !h.equals("7")) {
			throw new Exception("unexpected");
		}
`

const java = `
set -e
cat > main.java << 'EOF'
import java.sql.*;

public class main {
	public static void main(String[] args) throws Exception {

    %C

		%T
		
	}
}
EOF
# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.client.key -out key.pk8 -nocrypt

export PATH=$PATH:/usr/lib/jvm/java-1.7-openjdk/bin
javac main.java
java -cp /postgres.jar:. main
`

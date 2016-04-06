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

package acceptance

import (
	"strings"
	"testing"
)

func TestDockerJava(t *testing.T) {
	testDockerSuccess(t, "java", []string{"/bin/sh", "-c", strings.Replace(java, "%v", "Int(2, 3)", 1)})
	testDockerFail(t, "java", []string{"/bin/sh", "-c", strings.Replace(java, "%v", `String(2, "a")`, 1)})
}

const java = `
set -e
cat > main.java << 'EOF'
import java.sql.*;

public class main {
	public static void main(String[] args) throws Exception {
		Class.forName("org.postgresql.Driver");

		String DB_URL = "jdbc:postgresql://";
		DB_URL += System.getenv("PGHOST") + ":" + System.getenv("PGPORT");
		DB_URL += "/?ssl=true";
		DB_URL += "&sslcert=" + System.getenv("PGSSLCERT");
		DB_URL += "&sslkey=key.pk8";
		DB_URL += "&sslrootcert=/certs/ca.crt";
		DB_URL += "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory";
		Connection conn = DriverManager.getConnection(DB_URL);

		PreparedStatement stmt = conn.prepareStatement("CREATE DATABASE test");
		int res = stmt.executeUpdate();
		if (res != 0) {
		    throw new Exception("unexpected: CREATE DATABASE reports " + res + " rows changed, expecting 0");
		}

		stmt = conn.prepareStatement("CREATE TABLE test.f (x INT)");
		res = stmt.executeUpdate();
		if (res != 0) {
		    throw new Exception("unexpected: CREATE TABLE reports " + res + " rows changed, expecting 0");
		}

		stmt = conn.prepareStatement("INSERT INTO test.f VALUES (42)");
		res = stmt.executeUpdate();
		if (res != 1) {
		    throw new Exception("unexpected: INSERT reports " + res + " rows changed, expecting 1");
		}

		stmt = conn.prepareStatement("SELECT * FROM test.f");
		ResultSet rs = stmt.executeQuery();
		rs.next();
		int a = rs.getInt(1);
		if (a != 42) {
		    throw new Exception("unexpected: SELECT can't find inserted value: read " + a + ", expecting 42");
		}

		stmt = conn.prepareStatement("DROP TABLE test.f");
		res = stmt.executeUpdate();
		if (res != 0) {
		    throw new Exception("unexpected: DROP TABLE reports " + res + " rows changed, expecting 0");
		}

		stmt = conn.prepareStatement("SELECT 1, 2 > ?, ?::int, ?::string, ?::string, ?::string, ?::string, ?::string");
		stmt.setInt(1, 3);
		stmt.set%v;

		stmt.setBoolean(3, true);
		stmt.setLong(4, -4L);
		stmt.setFloat(5, 5.31f);
		stmt.setDouble(6, -6.21d);
		stmt.setShort(7, (short)7);

		rs = stmt.executeQuery();
		rs.next();
		a = rs.getInt(1);
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
	}
}
EOF
# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.key -out key.pk8 -nocrypt

export PATH=$PATH:/usr/lib/jvm/java-1.8-openjdk/bin
javac main.java
java -cp /postgres.jar:. main
`

#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

proc file_exists {filepath} {
  if {! [ file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
}

start_test "Ensure that 'debug sb recreate' works with row-level security"

spawn $argv demo --no-line-editor --empty --log-dir=logs
eexpect "defaultdb>"

send "CREATE TABLE rls1 (pk INT PRIMARY KEY, comment TEXT);\r"
send "ALTER TABLE rls1 ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY;\r"
send "CREATE POLICY pol1 ON rls1 USING (comment = 'visible');\r"
send "CREATE USER rls_user;\r"
send "ALTER TABLE rls1 OWNER TO rls_user;\r"
send "GRANT SYSTEM VIEWCLUSTERSETTING TO rls_user;\r"
send "GRANT SYSTEM VIEWACTIVITY to rls_user;\r"
send "GRANT SYSTEM VIEWSYSTEMTABLE to rls_user;\r"
send "SET ROLE rls_user;\r"
eexpect "defaultdb>"

send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM rls1;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set id1 $expect_out(1,string)
}

send "\\statement-diag download $id1\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$id1.zip"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle"
system "$python $pyfile stmt-bundle-$id1.zip bundle"

spawn $argv debug sb recreate bundle
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

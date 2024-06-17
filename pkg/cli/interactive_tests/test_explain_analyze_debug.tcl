#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

proc file_exists {filepath} {
  if {! [ file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
}

start_test "Ensure that EXPLAIN ANALYZE (DEBUG) works as expected in the sql shell"

start_server $argv

# Spawn a sql shell.
spawn $argv sql --no-line-editor
set client_spawn_id $spawn_id
eexpect root@

# Set a couple of cluster settings to ensure recreation of the bundle with them
# succeeds.
send "SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';\r"
eexpect "SET CLUSTER SETTING"
eexpect root@

send "SET CLUSTER SETTING sql.distsql.use_streamer.enabled = false;\r"
eexpect "SET CLUSTER SETTING"
eexpect root@

# Note: we need to use SELECT 1 (i.e. do not select a table)
# so that the "recreate" test below is a proper regression test
# for issue https://github.com/cockroachdb/cockroach/issues/86472.
send "EXPLAIN ANALYZE (DEBUG) SELECT 1;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set id1 $expect_out(1,string)
}

expect {
  "warning: pq: unexpected DataRow in simple query execution" {
    puts "Error: unexpected DataRow in simple query execution"
    exit 1
  }
  "connection lost" {
    puts "Error: connection lost"
    exit 1
  }
  "root@" {
  }
}

send "\\statement-diag list\r"
eexpect "Statement diagnostics bundles:"
eexpect "$id1"
eexpect "EXPLAIN"
eexpect root@

send "\\statement-diag download $id1\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$id1.zip"

send_eof
eexpect eof

end_test

stop_server $argv

start_test "Ensure that a bundle can be restarted from."

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle"
system "$python $pyfile stmt-bundle-$id1.zip bundle"

spawn $argv debug statement-bundle recreate bundle
eexpect "Statement was:"
eexpect "SELECT"
eexpect root@

send_eof
eexpect eof

end_test

start_test "Ensure that 'statement-bundle recreate' replaces placeholders with their values"

start_server $argv

# Spawn a sql shell.
spawn $argv sql --no-line-editor
set client_spawn_id $spawn_id
eexpect root@

# Delete bundles from the previous stmts if there are any.
send "DELETE FROM system.statement_diagnostics WHERE true;\r"
eexpect root@

send "CREATE TABLE t (k INT PRIMARY KEY);\r"
eexpect root@

send "PREPARE p AS SELECT * FROM t WHERE k = \$1;\r"
eexpect root@

send "SELECT crdb_internal.request_statement_bundle('SELECT * FROM t WHERE k = _', 0::FLOAT, 0::INTERVAL, 0::INTERVAL);\r"
eexpect root@

send "EXECUTE p(1);\r"
eexpect root@

# Figure out the ID of the bundle we just collected.
send "SELECT id FROM system.statement_diagnostics LIMIT 1;\r"
eexpect "LIMIT 1;"
expect -re "\r\n *(\\d+)" {
  set id2 $expect_out(1,string)
}
eexpect root@

send "\\statement-diag download $id2\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$id2.zip"

stop_server $argv

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle2"
system "$python $pyfile stmt-bundle-$id2.zip bundle2"

spawn $argv debug statement-bundle recreate bundle2
eexpect "Statement (had 1 placeholder) was:"
eexpect "SELECT * FROM t WHERE k = 1:::INT8;"
eexpect root@

send_eof
eexpect eof

end_test

start_server $argv

start_test "Ensure that EXPLAIN ANALYZE (DEBUG) works for a tenant"

start_tenant 5 $argv

spawn $argv sql --no-line-editor --port [tenant_port 5]

set client_spawn_id $spawn_id
eexpect root@


send "EXPLAIN ANALYZE (DEBUG) SELECT 1;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set id3 $expect_out(1,string)
}

expect {
  "warning: pq: unexpected DataRow in simple query execution" {
    puts "Error: unexpected DataRow in simple query execution"
    exit 1
  }
  "connection lost" {
    puts "Error: connection lost"
    exit 1
  }
  "root@" {
  }
}

send "\\statement-diag list\r"
eexpect "Statement diagnostics bundles:"
eexpect "$id3"
eexpect "EXPLAIN"
eexpect root@

send "\\statement-diag download $id3\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$id3.zip"

send_eof
eexpect eof

stop_tenant 5 $argv

end_test

stop_server $argv

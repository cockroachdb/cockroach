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
spawn $argv sql
set client_spawn_id $spawn_id
eexpect root@

send "EXPLAIN ANALYZE (DEBUG) SELECT 1;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set id $expect_out(1,string)
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

send "\\statement-diag download $id\r"
eexpect "Bundle saved to"

file_exists "stmt-bundle-$id.zip"

send_eof
eexpect eof

end_test

start_test "Ensure that EXPLAIN ANALYZE (DEBUG) works for a tenant"

start_tenant 5 $argv

spawn $argv sql --port [tenant_port 5]

set client_spawn_id $spawn_id
eexpect root@


send "EXPLAIN ANALYZE (DEBUG) SELECT 1;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set id $expect_out(1,string)
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

send "\\statement-diag download $id\r"
eexpect "Bundle saved to"

file_exists "stmt-bundle-$id.zip"

send_eof
eexpect eof

stop_tenant 5 $argv

end_test

stop_server $argv

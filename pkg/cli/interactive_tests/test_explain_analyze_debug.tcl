#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Ensure that EXPLAIN ANALYZE (DEBUG) works as expected in the sql shell"

start_server $argv

# Spawn a sql shell.
spawn $argv sql
set client_spawn_id $spawn_id
eexpect root@

send "EXPLAIN ANALYZE (DEBUG) SELECT 1;\r"
eexpect "Statement diagnostics bundle generated."
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

interrupt
eexpect eof

end_test

stop_server $argv

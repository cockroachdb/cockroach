#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

system "echo \"CREATE TABLE defaultdb.t(hello) AS SELECT 'world';\" >logs/init.sql"

set ::env(COCKROACH_INITIAL_SQL_DIR) "logs"
start_server $argv

spawn $argv sql
eexpect root@

# Check that the initial script has been run.
send "TABLE t;\r"
eexpect "hello"
eexpect "world"
eexpect root@
interrupt
eexpect eof

stop_server $argv

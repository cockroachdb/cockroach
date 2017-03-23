#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

send "$argv start --pid-file=server_pid --background\r"
eexpect "initialized"

eexpect ":/# "

# Test that dropping a table a node held a lease on before a restart doesn't
# hang. We use a SELECT statement to acquire a lease on the table.
send "$argv sql -e \"create database t; create table t.t (x INT); select * from t.t;\"\r"
eexpect "(0 rows)"

system "kill `cat server_pid`"
eexpect "initiating graceful shutdown"
eexpect "shutdown completed"

send "$argv start --pid-file=server_pid --background\r"
eexpect "restarted"

send "$argv sql -e \"drop table t.t;\"\r"
eexpect "DROP TABLE"

system "kill `cat server_pid`"

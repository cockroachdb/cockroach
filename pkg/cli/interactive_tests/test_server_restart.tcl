#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Test that dropping a table a node held a lease on before a restart doesn't
# hang. We use a SELECT statement to acquire a lease on the table.
send "$argv sql -e \"create database t; create table t.t (x INT); select * from t.t;\"\r"
eexpect "(0 rows)"

# Send an interrupt to allow the server to gracefully shutdown. We
# need a graceful shutdown in order for table leases to be released,
# otherwise the DROP TABLE below will have to wait for the table lease
# to expire (5 min).
interrupt
start_server $argv

send "$argv sql -e \"drop table t.t;\"\r"
eexpect "DROP TABLE"

stop_server $argv

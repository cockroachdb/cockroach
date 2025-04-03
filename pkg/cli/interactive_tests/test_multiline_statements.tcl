#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect "defaultdb>"

start_test "Test that a multi-line entry can be recalled escaped."
send "select 'foo\r"
eexpect " ->"
send "bar';\r"
eexpect "1 row"
eexpect "defaultdb>"

# Send up-arrow.
send "\033\[A"
eexpect "select 'foo"
eexpect " -> bar';"
send "\r"
eexpect "1 row"
eexpect "defaultdb>"

start_test "Test that Ctrl+C after the first line merely cancels the statement and presents the prompt."
send "\r"
eexpect defaultdb>
send "select\r"
eexpect " ->"
interrupt
eexpect "defaultdb>"
end_test

start_test "Test that a dangling table creation can be committed, and that other non-DDL, non-DML statements can be issued in the same txn. (#15283)"
send "create database if not exists t;"
send "drop table if exists t.blih;"
send "create table if not exists t.kv(k int primary key, v int);\r"
eexpect "CREATE TABLE"
eexpect "defaultdb>"
send "begin; set local autocommit_before_ddl=off; create table t.blih(x INT REFERENCES t.kv(k));\r\r"
eexpect "CREATE TABLE"
eexpect "defaultdb"
eexpect OPEN

send "show all cluster settings;\r"
eexpect "rows"
eexpect "defaultdb"
eexpect OPEN

send "commit;\r"
eexpect COMMIT
eexpect "defaultdb>"
end_test

send "quit\r"
eexpect eof


# we force TERM to xterm, otherwise we can't
# test bracketed paste below.
set env(TERM) xterm

spawn $argv sql
eexpect "defaultdb>"

start_test "Test that a multi-line bracketed paste is handled properly."
send "\033\[200~"
send "\\set display_format csv\r\n"
send "values (1,'a'), (2,'b'), (3,'c');\r\n"
send "\033\[201~\r\n"
eexpect "1,a"
eexpect "2,b"
eexpect "3,c"
eexpect "defaultdb>"
end_test

send_eof
eexpect eof

stop_server $argv

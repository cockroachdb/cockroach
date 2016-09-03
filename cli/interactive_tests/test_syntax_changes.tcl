#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Test disabled; see #9369
exit 0

spawn $argv sql
eexpect root@

send "set syntax=traditional;\r"
eexpect "SET"
eexpect root@

send "select 1\r"
eexpect " ->"
send "; set syntax = modern; select 1#;\r"
eexpect " ->"
send ";\r"
eexpect "1 row"
eexpect "1 row"
eexpect root@

send "set syntax=traditional;\r"
eexpect "SET"
eexpect root@

send "select 1\r"
eexpect " ->"
send "; set syntax=modern; select '''\r"
send "'; boo\r"
eexpect " ->"
send "''';\r"
eexpect "1 row"
eexpect "boo"
eexpect "1 row"
eexpect root@

send "\\q\r"
expect eof

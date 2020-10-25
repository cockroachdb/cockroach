#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check \c and \connect to connect another DB"

spawn $argv demo movr

eexpect "movr"

send "\\c postgres \r"
eexpect "SET"
eexpect "postgres"

send "\\connect movr \r"
eexpect "SET"
eexpect "movr"

end_test

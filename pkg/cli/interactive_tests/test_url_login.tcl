#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

start_test "Check that the client can start when no username is specified."
# This is run as an acceptance test to ensure that the code path
# that generates the interactive prompt presented to the user is
# also exercised by the test.
spawn $argv sql --url "postgresql://localhost:26257?sslmode=disable"
eexpect @localhost
end_test

send "\004"
eexpect eof

stop_server $argv

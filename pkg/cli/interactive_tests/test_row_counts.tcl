#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

start_test "Check that row counts are displayed when output is a terminal, even when --display-row-counts is not specified"
send "echo 'select 1;' | $argv sql\r"
eexpect "1 row"
eexpect ":/# "
end_test

start_test "Check that row counts are not displayed when output is not a terminal, when --display-row-counts is not specified"
send "echo 'select 1;' | $argv sql | cat\r"
expect {
    "1 row" {
	report "unexpected row count"
	exit 1
    }
    ":/# " {}
}
end_test

start_test "Check that row counts are displayed when output is not a terminal, when --display-row-counts is specified"
send "echo 'select 1;' | $argv sql | cat\r"
eexpect "1 row"
eexpect ":/# "
end_test

send "exit 0\r"
eexpect eof

stop_server $argv

#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

# Check table ASCII art with and without --format=pretty. (#7268)

start_test "Check that tables are pretty-printed when input is not a terminal but --format=pretty is specified."
send "echo 'select 1;' | $argv sql --format=pretty\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
eexpect ":/# "
end_test

start_test "Check that tables are not pretty-printed when input is not a terminal and --format=pretty is not specified."
send "echo begin; echo 'select 1;' | $argv sql\r"
eexpect "begin\r\n1 row\r\n1\r\n1\r\n"
end_test

start_test "Check that tables are pretty-printed when input is a terminal and --format=pretty is not specified."
send "$argv sql\r"
eexpect root@
send "select 1;\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
send "\\q\r"
eexpect ":/# "
end_test

start_test "Check that tables are not pretty-printed when input is a terminal and --format=tsv is specified."
send "$argv sql --format=tsv\r"
eexpect root@
send "select 42; select 1;\r"
eexpect "42\r\n1 row\r\n1\r\n1\r\n"
eexpect root@
send "\\q\r"
end_test

eexpect ":/# "
send "exit\r"
eexpect eof

stop_server $argv

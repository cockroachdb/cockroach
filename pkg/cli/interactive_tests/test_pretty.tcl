#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

# Check table ASCII art with and without --format=pretty. (#7268)

start_test "Check that tables are pretty-printed when output is not a terminal but --format=pretty is specified."
send "echo 'select 1;' | $argv sql --format=pretty | cat\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
eexpect ":/# "
end_test

start_test "Check that tables are pretty-printed when input is not a terminal and --format=pretty is not specified, but output is a terminal."
send "echo begin; echo 'select 1;' | $argv sql\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
eexpect ":/# "
end_test

start_test "Check that tables are not pretty-printed when output is not a terminal and --format=pretty is not specified"
send "echo begin; echo 'select 1;' | $argv sql | cat\r"
eexpect "begin\r\n1\r\n1\r\n"
eexpect ":/# "
end_test

start_test "Check that tables are pretty-printed when input and output are a terminal and --format=pretty is not specified."
send "$argv sql\r"
eexpect root@
send "select 1;\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
eexpect root@
end_test

start_test "Check that the shell supports unicode input and that results display unicode characters."
send "select '☃';\r"
eexpect "U00002603"
eexpect "☃"
eexpect "+-*+\r\n*1 row"
eexpect root@
end_test

start_test "Check that tables are not pretty-printed when output is a terminal and --format=tsv is specified."
send "\\q\r"
eexpect ":/# "
send "$argv sql --format=tsv\r"
eexpect root@
send "select 42; select 1;\r"
eexpect "42\r\n42\r\n"
eexpect "1\r\n1\r\n"
eexpect root@
send "\\q\r"
end_test

eexpect ":/# "
send "exit\r"
eexpect eof

stop_server $argv

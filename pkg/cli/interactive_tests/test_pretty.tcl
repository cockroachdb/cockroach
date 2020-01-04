#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

# Check table ASCII art with and without --format=table. (#7268)

start_test "Check that tables are pretty-printed when output is not a terminal but --format=table is specified."
send "echo 'select 1 as WOO;' | $argv sql --format=table | cat\r"
eexpect "woo"
eexpect "?-*-\r\n"
eexpect "  1"
eexpect "1 row"
eexpect ":/# "
end_test

start_test "Check that tables are pretty-printed when input is not a terminal and --format=table is not specified, but output is a terminal."
send "echo begin; echo 'select 1 as WOO;' | $argv sql\r"
eexpect "woo"
eexpect "?-*-\r\n"
eexpect "  1"
eexpect "1 row"
eexpect ":/# "
end_test

start_test "Check that tables are not pretty-printed when output is not a terminal and --format=table is not specified"
send "echo begin; echo 'select 1 as woo;' | $argv sql | cat\r"
eexpect "begin\r\nwoo\r\n1\r\n"
eexpect ":/# "
end_test

start_test "Check that tables are pretty-printed when input and output are a terminal and --format=table is not specified."
send "$argv sql\r"
eexpect root@
send "select 1 as WOO;\r"
eexpect "?-*-\r\n"
eexpect "  1"
eexpect "1 row"
eexpect root@
end_test

start_test "Check that the shell supports unicode input and that results display unicode characters."
send "select '☃';\r"
eexpect "?column?"
eexpect "?-*-\r\n"
eexpect "☃"
eexpect "1 row"
eexpect root@
end_test

send "\\q\r"
eexpect ":/# "

start_test "Check that tables are not pretty-printed when output is a terminal and --format=tsv is specified."
send "$argv sql --format=tsv\r"
eexpect root@
send "select 42 as woo; select 1 as woo;\r"
eexpect "woo\r\n42\r\n"
eexpect "woo\r\n1\r\n"
eexpect root@
send "\\q\r"
eexpect ":/# "
end_test

start_test "Check that a warning is printed if the pretty-printer is buffering many rows."
send "$argv sql --format=table -e 'select * from generate_series(1,20000)' >/dev/null\r"
eexpect "warning: buffering more than"
eexpect "RAM usage growing"
eexpect "consider another formatter"
eexpect ":/# "
end_test

send "exit\r"
eexpect eof

stop_server $argv

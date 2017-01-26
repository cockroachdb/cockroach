#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

# Check table ASCII art with and without --format=pretty. (#7268)

# Check that tables are pretty-printed when input is not a terminal
# but --format=pretty is specified.
send "echo 'select 1;' | $argv sql --format=pretty\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
eexpect ":/# "

# Check that tables are not pretty-printed when input is not a terminal
# and --format=pretty is not speciifed.
send "echo begin; echo 'select 1;' | $argv sql $silence_prop_eval_kv\r"
eexpect "begin\r\n1 row\r\n1\r\n1\r\n"

# Check that tables are pretty-printed when input is a terminal
# and --format=pretty is not specified.
send "$argv sql\r"
eexpect root@
send "select 1;\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
send "\\q\r"
eexpect ":/# "

# Check that tables are not pretty-printed when input is a terminal
# and --format=tsv is specified.
send "$argv sql --format=tsv\r"
eexpect root@
send "select 42; select 1;\r"
eexpect "42\r\n1 row\r\n1\r\n1\r\n"
eexpect root@
send "\\q\r"

eexpect ":/# "
send "exit\r"
eexpect eof

stop_server $argv


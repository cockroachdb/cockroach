#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
eexpect ":/# "

# Check table ASCII art with and without --pretty. (#7268)

# Check that tables are pretty-printed when input is not a terminal
# but --pretty is specified.
send "echo 'select 1;' | $argv sql --pretty\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
eexpect ":/# "

# Check that tables are not pretty-printed when input is not a terminal
# and --pretty is not speciifed.
send "echo begin; echo 'select 1;' | $argv sql\r"
eexpect "begin\r\n1 row\r\n1\r\n1\r\n"

# Check that tables are pretty-printed when input is a terminal
# and --pretty is not specified.
send "$argv sql\r"
eexpect root@
send "select 1;\r"
eexpect "+-*+\r\n*\r\n+-*+\r\n*1 row"
send "\\q\r"
eexpect ":/# "

# Check that tables are not pretty-printed when input is a terminal
# and --pretty=false is specified.
send "$argv sql --pretty=false\r"
eexpect root@
send "select 42; select 1;\r"
eexpect "42\r\n1 row\r\n1\r\n1\r\n"
eexpect root@
send "\\q\r"

eexpect ":/# "
send "exit\r"
eexpect eof

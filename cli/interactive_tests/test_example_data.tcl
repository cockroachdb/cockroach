#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Check that the startrek example can be loaded in the db.
send "$argv gen example-data startrek | $argv sql\r"
eexpect ":/# "
send "echo \$?\r"
eexpect "0\r\n:/# "

# Check that the startrek example is loaded.
send "$argv sql -e 'SELECT COUNT(*) FROM startrek.quotes'\r"
eexpect "COUNT"
eexpect "200"
eexpect "1 row"
eexpect ":/# "

# Check that the intro example can be loaded in the db.
send "$argv gen example-data intro | $argv sql\r"
eexpect ":/# "
send "echo \$?\r"
eexpect "0\r\n:/# "

# Check that the startrek example is loaded.
send "$argv sql -e 'SELECT COUNT(*) FROM intro.mytable'\r"
eexpect "COUNT"
eexpect "42"
eexpect "1 row"
eexpect ":/# "

# Clean up.
send "exit 0\r"
eexpect eof

stop_server $argv

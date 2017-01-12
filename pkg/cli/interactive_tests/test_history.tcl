#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

# Ensure the connection is up and working.
send "select 1;\r"
eexpect "1 row"
eexpect root@

# Test that last line can be recalled with arrow-up
send "\033\[A"
eexpect "SELECT 1;"

# Test that recalled last line can be executed
send "\r"
eexpect "1 row"
eexpect root@

# Test that we can recall a previous line with Ctrl+R
send "foo;\r"
eexpect "syntax error"
eexpect root@
send "\022sel"
eexpect "SELECT 1;"

# Test that recalled previous line can be executed
send "\r"
eexpect "1 row"
eexpect root@

# Test that last recalled line becomes top of history
send "\033\[A"
eexpect "SELECT 1;"

# Test that client cannot terminate with Ctrl+D while cursor
# is on recalled line
send "\004"
send "\r"
eexpect "1 row"
eexpect root@

# Test that Ctrl+D does terminate client on empty line
send "\004"
eexpect eof

# Test that history is preserved across runs
spawn $argv sql
eexpect root@
send "\033\[A"
eexpect "SELECT 1;"

# Test that the client cannot terminate with Ctrl+C while
# cursor is on recalled line
interrupt
send "\rselect 1;\r"
eexpect "1 row"
eexpect root@

# Finally terminate with Ctrl+C
interrupt
eexpect eof

stop_server $argv

#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Check that by default, an error prevents subsequent statements from running.
send "(echo 'select foo;'; echo 'select 1;') | $argv sql\r"
eexpect "pq: column name \"foo\" not found\r\nError: pq: column name"
eexpect ":/# "
send "echo \$?\r"
eexpect "1\r\n:/# "

# Check that a user can request to continue upon failures.
send "(echo '\\unset errexit'; echo 'select foo;'; echo 'select 1;') | $argv sql\r"
eexpect "pq: column name \"foo\" not found"
eexpect "1 row"
eexpect ":/# "
send "echo \$?\r"
eexpect "0\r\n:/# "

send "$argv sql\r"
eexpect "root@"

# Check that by default, an error does not cause an interactive failure.
send "select foo;\r"
eexpect "pq: column name"
eexpect "root@"

# Check that the user can ask for errors to terminate the interactive client.
send "\\set errexit\r"
eexpect "root@"
send "select foo;\r"
eexpect "Error: pq: column name"
eexpect ":/# "
send "echo \$?\r"
eexpect "1\r\n:/# "

send "exit 0\r"
eexpect eof

stop_server $argv

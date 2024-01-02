#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

start_test "Ensure that replication mode works as expected in the sql shell"

# Spawn a sql shell.
spawn /bin/bash

send "$argv sql --url `cat server_url`'\&replication=database' -e 'IDENTIFY_SYSTEM'\r"
eexpect "(1 row)"

send_eof
eexpect eof

end_test

stop_server $argv

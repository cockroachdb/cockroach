#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

start_test "Check that the client can start when no username is specified."
# This is run as an acceptance test to ensure that the code path
# that generates the interactive prompt presented to the user is
# also exercised by the test.
spawn $argv sql --url "postgresql://localhost:26257?sslmode=disable"
eexpect @localhost
send_eof
eexpect eof

end_test

stop_server $argv

start_test "Check that the unix socket can be used simply."

# Start a server with a socket listener.
set mywd [pwd]

system "$argv start-single-node --insecure --pid-file=server_pid --socket-dir=. --background -s=path=logs/db >>logs/expect-cmd.log 2>&1;
        $argv sql --insecure -e 'select 1'"

spawn $argv sql --url "postgresql://?host=$mywd&port=26257"
eexpect root@
send_eof
eexpect eof

stop_server $argv

end_test

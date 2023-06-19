#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

start_test "Check that the client can start when no username is specified."
# This is run as an acceptance test to ensure that the code path
# that generates the interactive prompt presented to the user is
# also exercised by the test.
spawn $argv sql --no-line-editor --url "postgresql://localhost:26257?sslmode=disable"
eexpect @localhost
send_eof
eexpect eof

end_test

stop_server $argv

# Start a server with a socket listener.
set mywd [pwd]

system "$argv start-single-node --insecure --pid-file=server_pid --socket-dir=. --background -s=path=logs/db >>logs/expect-cmd.log 2>&1;
        $argv sql --insecure -e 'select 1'"

start_test "Check that the unix socket can be used simply."

spawn $argv sql --no-line-editor --url "postgresql://?host=$mywd&port=26257"
eexpect root@

end_test


start_test "Check that the prompt can be customized to display a socket-based conn."
send "\\set prompt1 abc%Mdef\r"
eexpect "abc\\\[local:$mywd\\\]def"

send "\\set prompt1 abc%mdef\r"
expect "abc\\\[local\\\]def"

send "\\set prompt1 abc%>def\r"
eexpect "abc26257def"
end_test

send_eof
eexpect eof

stop_server $argv

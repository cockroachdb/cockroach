#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect "defaultdb>"

start_test "Check that interrupt with a partial line clears the line."
send "asasaa"
eexpect "asasaa"
interrupt
eexpect "defaultdb>"
end_test

start_test "Check that interrupt with a multiline clears the current input."
send "select\r"
eexpect " -> "
send "'XXX'"
interrupt
eexpect "defaultdb>"
end_test

start_test "Check that interrupt with an empty line prints an information message."
interrupt
eexpect "terminate input to exit"
eexpect "defaultdb>"
end_test

start_test "Check that interrupt can cancel a query."
send "select pg_sleep(1000000);\r"
eexpect "\r\n"
sleep 0.4
interrupt
eexpect "query execution canceled"
eexpect "57014"

eexpect "defaultdb>"
end_test

# Quit the SQL client.
send_eof
eexpect eof

start_test "Check that interrupt without a line editor quits the shell."
# regression test for #90653.
spawn $argv sql --no-line-editor
eexpect "defaultdb>"
interrupt
set timeout 1
expect {
    "attempting to cancel query" { exit 1 }
    "panic: runtime error" { exit 1 }
    timeout {}
}
set timeout 30
interrupt
expect {
    "attempting to cancel query" { exit 1 }
    "panic: runtime error" { exit 1 }
    "" {}
}
end_test
send "\rexit\r"
eexpect eof

# Open a unix shell.
spawn /bin/bash
set shell2_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that interactive, non-terminal queries are cancellable without terminating the shell."
send "$argv sql >/dev/null\r"
eexpect "\r\n"
sleep 0.4
send "select 'A'+3;\r"
eexpect "\r\n"
# Sanity check: we can still process _some_ SQL.
# stderr is not redirected so we still see errors.
eexpect "unsupported binary operator"

# Now on to check cancellation.
send "select pg_sleep(10000);\r"
sleep 0.4
interrupt
eexpect "query execution canceled"
eexpect "57014"

# Send another query, expect an error. The shell should
# not have terminated by this point.
send "select 'A'+3;\r"
eexpect "\r\n"
eexpect "unsupported binary operator"

# Terminate SQL shell, expect unix shell.
send_eof
eexpect ":/# "

start_test "Check that non-interactive interrupts terminate the SQL shell."
send "cat | $argv sql\r"
eexpect "\r\n"
sleep 0.4
# Sanity check.
send "select 'XX'||'YY';\r"
eexpect "XXYY"

# Check what interrupt does.
send "select pg_sleep(10000);\r"
sleep 0.4
interrupt
# This exits the SQL shell directly. expect unix shell.
eexpect ":/# "

end_test

send "exit 0\r"
eexpect eof

stop_server $argv

# Regression test for #92943.
start_test "Check that SIGTERM sent to an interactive shell terminates the shell."

spawn $argv demo --empty --pid-file=demo_pid --log-dir=logs
eexpect "Welcome"
eexpect root@
eexpect "defaultdb>"

system "kill -TERM `cat demo_pid`"

eexpect eof

end_test

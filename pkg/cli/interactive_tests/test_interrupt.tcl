#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect "defaultdb>"

start_test "Check that interrupt with a partial line clears the line."
send "asasaa"
interrupt
eexpect "defaultdb>"
end_test

start_test "Check that interrupt with a multiline clears the current line."
send "select\r"
eexpect " -> "
send "'XXX'"
interrupt
send "'YYY';\r"
eexpect "column"
eexpect "YYY"
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

# TODO(knz): we currently need to trigger a reconnection
# before we get a healthy prompt. This will be fixed
# in a later version.
send "\r"
eexpect "defaultdb>"
end_test

# Quit the SQL client, and open a unix shell.
send_eof
eexpect eof
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

# TODO(knz): we currently need to trigger a reconnection
# before we get a healthy prompt. This will be fixed
# in a later version.
send "\rselect 1;\r"

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

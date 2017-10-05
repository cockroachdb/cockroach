#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

# Ensure the connection is up and working.
send "select 1;\r"
eexpect "1 row"
eexpect root@

start_test "Test that last line can be recalled with arrow-up"
send "\033\[A"
eexpect "select 1;"
end_test

start_test "Test that recalled last line can be executed"
send "\r"
eexpect "1 row"
eexpect root@
end_test

start_test "Test that we can recall a previous line with Ctrl+R"
send "foo;\r"
eexpect "syntax error"
eexpect root@
send "\022sel"
eexpect "select 1;"
end_test

start_test "Test that recalled previous line can be executed"
send "\r"
eexpect "1 row"
eexpect root@
end_test

start_test "Test that last recalled line becomes top of history"
send "\033\[A"
eexpect "select 1;"
end_test

start_test "Test that client cannot terminate with Ctrl+D while cursor is on recalled line"
send_eof
send "\r"
eexpect "1 row"
eexpect root@
end_test

start_test "Test that Ctrl+D does terminate client on empty line"
send_eof
eexpect eof
end_test

start_test "Test that history is preserved across runs"
spawn $argv sql
eexpect root@
send "\033\[A"
eexpect "select 1;"
end_test

start_test "Test that the client cannot terminate with Ctrl+C while cursor is on recalled line"
interrupt
send "\rselect 1;\r"
eexpect "1 row"
eexpect root@
end_test

start_test "Test that two statements on the same line can be recalled together."
send "select 2; select 3;\r"
eexpect "1 row"
eexpect "1 row"
eexpect root@
send "\033\[A"
eexpect "select 2; select 3;"
send "\r"
eexpect "1 row"
eexpect "1 row"
eexpect root@
end_test

# Finally terminate with Ctrl+C
interrupt
eexpect eof

stop_server $argv

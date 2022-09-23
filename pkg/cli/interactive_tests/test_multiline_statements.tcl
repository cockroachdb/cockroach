#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

# we force TERM to xterm, otherwise we can't
# test bracketed paste below.
set env(TERM) xterm

spawn $argv sql
eexpect root@

start_test "Test that a multi-line entry can be recalled escaped."
send "select 'foo\r"
eexpect " ->"
send "bar';\r"
eexpect "1 row"
eexpect "root@"

# Send up-arrow.
send "\033\[A"
eexpect "select 'foo"
eexpect " -> bar';"
send "\r"
eexpect "1 row"
eexpect "root@"

start_test "Test that Ctrl+C after the first line merely cancels the statement and presents the prompt."
send "\r"
eexpect root@
send "select\r"
eexpect " ->"
interrupt
eexpect root@
end_test

# Pending the bubbletea changes here: https://github.com/charmbracelet/bubbletea/issues/404
# start_test "Test that a multi-line bracketed paste is handled properly."
# send "\033\[200~"
# send "\\set display_format csv\r\n"
# send "values (1,'a'), (2,'b'), (3,'c');\r\n"
# send "\033\[201~\r\n"
# eexpect "1,a"
# eexpect "2,b"
# eexpect "3,c"
# eexpect root@
# end_test

send_eof
eexpect eof

stop_server $argv

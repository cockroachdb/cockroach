#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

# Check that syntax errors are handled client-side when running interactive.
send "begin;\r\r"
eexpect BEGIN
eexpect root@

send "select 3+;\r"
eexpect "statement ignored"
eexpect root@

send "select 1;\r"
eexpect "1 row"
eexpect root@

send "commit;\r"
eexpect COMMIT
eexpect root@

# Check that the user can force server-side handling.
send "\\unset check_syntax\r"
eexpect root@

send "begin;\r"
eexpect BEGIN

send "select 3+;\r"
eexpect "pq: syntax error"
eexpect root@

send "select 1;\r"
eexpect "current transaction is aborted"
eexpect root@

send "commit;\r"
eexpect "ROLLBACK"
eexpect root@

interrupt
eexpect eof

# Check that syntax errors are handled server-side by default when running non-interactive.
spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

send "(echo '\\unset errexit'; echo 'begin;'; echo 'select 1+;'; echo 'select 1;') | $argv sql\r"
eexpect "syntax error"
eexpect "current transaction is aborted"
eexpect ":/# "
send "echo \$?\r"
eexpect "1\r\n:/# "

send "(echo '\\unset errexit'; echo '\\set check_syntax'; echo 'begin;'; echo 'select 1+;'; echo 'select 1;'; echo 'commit;') | $argv sql\r"
eexpect "syntax error"
eexpect "1 row"
eexpect "COMMIT"
eexpect ":/# "
send "echo \$?\r"
eexpect "0\r\n:/# "

send "exit 0\r"
eexpect eof

stop_server $argv


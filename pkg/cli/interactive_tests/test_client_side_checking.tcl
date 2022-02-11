#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

start_test "Check that syntax errors are handled client-side when running interactive."
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
end_test

start_test "Check that the syntax checker does not get confused by empty inputs."
# (issue #22441.)
send ";\r"
eexpect "0 rows"
eexpect root@
end_test

start_test "Check that the user can force server-side handling."
send "\\unset check_syntax\r"
eexpect root@

send "begin;\r"
eexpect BEGIN

send "select 3+;\r"
eexpect "ERROR: at or near"
eexpect "syntax error"
eexpect root@

send "select 1;\r"
eexpect "current transaction is aborted"
eexpect root@

send "commit;\r"
eexpect "ROLLBACK"
eexpect root@

send_eof
eexpect eof
end_test

start_test "Check that syntax errors are handled server-side by default when running non-interactive."
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
end_test

start_test "Check that --debug-sql-cli sets suitable simplified client-side options."
send "$argv sql --debug-sql-cli\r"
eexpect "Welcome"
eexpect "root@"
send "\\set display_format csv\r\\set\r"
eexpect "check_syntax,false"
eexpect "echo,true"
eexpect "prompt1,%n@%M>"
eexpect "root@"
send "\\q\r"
eexpect ":/# "
end_test

send "exit 0\r"
eexpect eof

stop_server $argv

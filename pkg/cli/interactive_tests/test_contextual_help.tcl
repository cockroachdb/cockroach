#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql

start_test "Check that a syntax error can make suggestions."
send "select * from;\r"
eexpect "syntax error"
eexpect "HINT: try \\\\h <SOURCE>"
eexpect root@
end_test

start_test "Check that a standalone '??' prints all help."
send "??\r"
eexpect "TRUNCATE"
eexpect "SHOW"
eexpect "ROLLBACK"
eexpect root@

send "??\t"
eexpect "TRUNCATE"
eexpect "SHOW"
eexpect "ROLLBACK"
eexpect "??"
send "\010\010"
send "select 1;\r"
eexpect "1 row"
eexpect root@
end_test

start_test "Check that a ?? after a simple statement prints help."
send "select ??\r"
eexpect "Command: "
eexpect "SELECT"
eexpect "data manipulation"
eexpect "FROM"
eexpect "ORDER BY"
eexpect "See also"
eexpect root@

send "select * from ??\r"
eexpect "Command: "
eexpect "data source"
eexpect "JOIN"
eexpect "EXPLAIN"
eexpect "SHOW"
eexpect "See also"
eexpect root@

end_test

start_test "Check that the last statement with help text made it to history."
send "\033\[A"
eexpect "select"
eexpect "from ??"
send "\r"
eexpect "See also"
eexpect root@
end_test

start_test "Check that a useful reminder is given if the user mistakenly uses a single ?."
send "select ?\r"
eexpect "Note:"
eexpect JSON
eexpect "use '??'"
eexpect " ->"
# restore the normal state
send ";\r"
eexpect HINT
eexpect root@
end_test


start_test "Check that ??-tab works."
send "select ??\t"
eexpect "Command: "
eexpect "SELECT"
eexpect "data manipulation"
eexpect "FROM"
eexpect "ORDER BY"
eexpect "See also"
eexpect "select ??"
send "\010\010"
send "1;\r"
eexpect "1 row"
eexpect root@

send "select * from ??\t"
eexpect "Command: "
eexpect "data source"
eexpect "JOIN"
eexpect "EXPLAIN"
eexpect "SHOW"
eexpect "See also"
eexpect "select * from ??"
send "\010\010"
send "(values (1));\r"
eexpect "1 row"
eexpect root@

end_test

start_test "Check that a ?? in a function call context prints help about that function."

send "select count(??\r"
eexpect "Function: "
eexpect "count"
eexpect "number of selected elements"
eexpect "Signature"
eexpect "See also"
eexpect root@

send "select count(??\t"
eexpect "Function: "
eexpect "count"
eexpect "number of selected elements"
eexpect "Signature"
eexpect "See also"
eexpect "select count(??"
send "\010\010"
send "1);\r"
eexpect "1 row"
eexpect root@

end_test

# Finally terminate with Ctrl+C.
interrupt
eexpect eof

start_test "Check that the hint for a single ? is also printed in non-interactive sessions."
spawn /bin/bash

send "echo '?' | $argv sql\r"
eexpect "Note:"
eexpect JSON
eexpect "use '??'"

send "exit\r"
eexpect eof
end_test

stop_server $argv

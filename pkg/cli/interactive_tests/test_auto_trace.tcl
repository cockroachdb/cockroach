#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

start_test "Check traces over simple statements"
send "\\set auto_trace\r"
eexpect root@
send "select 1;\r"
# main result
eexpect "1 row"
# trace result
eexpect "session recording"
eexpect "executing: SELECT 1"
eexpect "rows affected: 1"
eexpect root@
end_test

start_test "Check traces over simple statements with errors"
send "select woo;\r"
# main result
eexpect "column \"woo\" does not exist"
# trace result
eexpect "session recording"
eexpect "executing: SELECT woo"
eexpect "does not exist"
eexpect root@
end_test

start_test "Check results in simple traces"
send "\\reset auto_trace\r"
eexpect root@
send "create table t(x int); insert into t values (1),(2);\r"
eexpect root@
send "\\set auto_trace=results\r"
eexpect root@
send "select * from t;\r"
# main result
eexpect "2 rows"
# trace result
eexpect "session recording"
eexpect "executing: SELECT \* FROM t"
eexpect "output row:"
eexpect "output row:"
eexpect "rows affected: 2"
eexpect root@
end_test

start_test "Check KV traces"
send "\\set auto_trace=kv\r"
eexpect root@
send "select * from t;\r"
# main result
eexpect "2 rows"
# trace result
eexpect "querying next range"
eexpect "Scan"
eexpect "rows affected: 2"
eexpect root@
end_test

start_test "Check results in KV traces"
send "\\set auto_trace=kv,results\r"
eexpect root@
send "select * from t;\r"
# main result
eexpect "2 rows"
# trace result
eexpect "querying next range"
eexpect "Scan"
eexpect "output row:"
eexpect "output row:"
eexpect "rows affected: 2"
eexpect root@
end_test

# Terminate.
send "\\q\r"
eexpect eof

stop_server $argv

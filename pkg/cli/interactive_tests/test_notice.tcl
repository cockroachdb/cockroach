#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# This test ensures notices are being sent as expected.

spawn $argv demo --no-example-database
eexpect root@

start_test "Test that notices always appear at the end after all results."
send "SELECT IF(@1=4,crdb_internal.notice('hello'),@1) AS MYRES FROM generate_series(1,10);\r"
eexpect myres
eexpect 1
eexpect 10
eexpect "10 rows"
eexpect "NOTICE: hello"
eexpect root@

# Ditto with multiple result sets. Notices after all result sets.
send "SELECT crdb_internal.notice('hello') AS STAGE1;"
send "SELECT crdb_internal.notice('world') AS STAGE2;\r"
send "SELECT crdb_internal.notice('warning', 'stay indoors') AS STAGE3;\r"
eexpect stage1
eexpect stage2
eexpect "NOTICE: hello"
eexpect "NOTICE: world"
eexpect "WARNING: stay indoors"
eexpect root@
send_eof
eexpect eof
end_test

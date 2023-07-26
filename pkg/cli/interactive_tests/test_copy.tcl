#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql --no-line-editor
eexpect root@

send "drop table if exists t;\r"
eexpect "DROP TABLE"
eexpect root@
send "create table t (id INT PRIMARY KEY, t TEXT);\r"
eexpect "CREATE TABLE"
eexpect root@

start_test "Check that errors are reported as appropriate."

send "COPY invalid_table FROM STDIN;\r"
eexpect "ERROR: relation \"invalid_table\" does not exist"
eexpect root@

send "COPY t FROM STDIN;\r"
eexpect "Enter data to be copied followed by a newline."
eexpect "End with a backslash and a period on a line by itself, or an EOF signal."
eexpect ">>"

send "invalid text field\ttext with semicolon;\r"
send "\\.\r"

eexpect "could not parse"

end_test

start_test "check EMPTY copy"

send "COPY t FROM STDIN;\r"
eexpect ">>"
send_eof
eexpect "COPY 0"
eexpect root@

send "COPY t FROM STDIN;\r"
eexpect ">>"
send "\\.\r"
eexpect "COPY 0"
eexpect root@

end_test

start_test "multi statement with COPY"
send "SELECT 1; COPY t FROM STDIN CSV;\r"
eexpect "COPY together with other statements in a query string is not supported"
eexpect root@
send "COPY t FROM STDIN CSV;SELECT 1;\r"
eexpect "COPY together with other statements in a query string is not supported"
eexpect root@
send "COPY t TO STDOUT CSV;SELECT 1;\r"
eexpect "COPY together with other statements in a query string is not supported"
eexpect root@
send "SELECT 1;COPY t TO STDOUT CSV;\r"
eexpect "COPY together with other statements in a query string is not supported"
eexpect root@
end_test

start_test "Copy in transaction"
send "BEGIN;\r"
eexpect root@
send "COPY t FROM STDIN CSV;\r"
eexpect ">>"
send "11,cat\r"
send "12,dog\r"
send "\\.\r"

eexpect "COPY 2"
eexpect root@
send "COMMIT;\r"
eexpect root@
end_test

start_test "Check EOF and \. works as appropriate during COPY"

send "COPY t FROM STDIN CSV;\r"
eexpect ">>"
send "1,text with semicolon;\r"
eexpect ">>"
send "2,beat chef@;\r"
eexpect ">>"
send "3,more&text\r"
eexpect ">>"
send "\\.\r"

eexpect "COPY 3"
eexpect root@

# Try \copy as well.
send "\\copy t FROM STDIN CSV INVALID;\r"
eexpect "syntax error"
eexpect root@

send "\\copy t FROM STDIN CSV;\r"
eexpect ">>"
send "4,epa! epa!\r"
send_eof

eexpect "COPY 1"
eexpect root@

send "COPY t TO STDOUT;\r"

eexpect "1\ttext with semicolon;"
eexpect "2\tbeat chef@;"
eexpect "3\tmore&text"
eexpect "4\tepa! epa!"
eexpect "11\tcat"
eexpect "12\tdog"

eexpect root@

end_test

send_eof
eexpect eof

spawn $argv sql --no-line-editor
eexpect root@

## The following test can be re-enabled after fixing this issue:
# https://github.com/cockroachdb/cockroach/issues/93053

# start_test "check CTRL+C during COPY exits the COPY mode as appropriate"
#
# send "COPY t FROM STDIN CSV;\r"
# eexpect ">>"
# send "5,cancel me\r"
#
# interrupt
#
# eexpect "ERROR: COPY canceled by user"
# eexpect root@
#
# send "SELECT * FROM t ORDER BY id ASC;\r"
# eexpect "(6 rows)"
# eexpect root@
#
# end_test

send "truncate table t;\r"
eexpect "TRUNCATE"
eexpect root@

send_eof
eexpect eof

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Test file input invalid"

send "cat >/tmp/test_copy.sql <<EOF\r"
send "COPY t FROM STDIN CSV;\r"
send "invalid text;,t\r"
send "EOF\r"
eexpect ":/# "
send "$argv sql --insecure -f /tmp/test_copy.sql\r"
eexpect "ERROR: could not parse"
eexpect ":/# "
send "$argv sql --insecure < /tmp/test_copy.sql\r"
eexpect "ERROR: could not parse"
eexpect ":/# "

end_test

start_test "Test file input with valid content"

send "cat >/tmp/test_copy.sql <<EOF\r"
send "COPY t FROM STDIN CSV;\r"
send "1,a\r"
send "\\.\r\r"
send "COPY t FROM STDIN;\r"
send "EOF\r"
# Tab doesn't work nicely in cat<<EOF, so use echo to append the character with tab.
send "echo -e '2\\tb' >> /tmp/test_copy.sql\r"
eexpect ":/# "
send "$argv sql --insecure -f /tmp/test_copy.sql\r"
eexpect ":/# "
send "$argv sql --insecure -e 'COPY t TO STDOUT'\r"
eexpect "1\ta"
eexpect "2\tb"
eexpect ":/# "

send "$argv sql --insecure -e 'TRUNCATE TABLE t'\r"
eexpect ":/# "
send "$argv sql --insecure < /tmp/test_copy.sql\r"
eexpect ":/# "
send "$argv sql --insecure -e 'SELECT * FROM t ORDER BY id'\r"
eexpect "1 | a"
eexpect "2 | b"
eexpect "(2 rows)"
eexpect ":/# "

end_test

stop_server $argv

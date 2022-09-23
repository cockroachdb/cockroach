#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql --no-line-editor
eexpect root@

send "DROP TABLE IF EXISTS t;\r"
send "CREATE TABLE t (id INT PRIMARY KEY, t TEXT);\r"

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

start_test "multi statement with COPY"
send "SELECT 1; COPY t FROM STDIN CSV;\r"
eexpect "COPY together with other statements in a query string is not supported"
eexpect root@
send "COPY t FROM STDIN CSV;SELECT 1;\r"
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
send "2,beat chef@;\r"
send "3,more&text\r"
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

send "SELECT * FROM t ORDER BY id ASC;\r"

eexpect "1 | text with semicolon;"
eexpect "2 | beat chef@;"
eexpect "3 | more&text"
eexpect "4 | epa! epa!"
eexpect "11 | cat"
eexpect "12 | dog"

eexpect "(6 rows)"

eexpect root@

end_test

send_eof
eexpect eof

spawn $argv sql
eexpect root@

start_test "check CTRL+C during COPY exits the COPY mode as appropriate"

send "COPY t FROM STDIN CSV;\r"
eexpect ">>"
send "5,cancel me\r"

interrupt

eexpect "ERROR: COPY canceled by user"
eexpect root@

send "SELECT * FROM t ORDER BY id ASC;\r"
eexpect "(6 rows)"
eexpect root@

send "TRUNCATE TABLE t;\r"
eexpect root@

end_test

send_eof
eexpect eof


spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Test file input invalid"

send "cat >/tmp/test_copy.sql <<EOF\rCOPY t FROM STDIN CSV;\rinvalid text;,t\rEOF\r"
eexpect ":/# "
send "$argv sql --insecure -f /tmp/test_copy.sql\r"
eexpect "ERROR: could not parse"
eexpect ":/# "
send "$argv sql --insecure < /tmp/test_copy.sql\r"
eexpect "ERROR: could not parse"
eexpect ":/# "

end_test

start_test "Test file input with valid content"

send "cat >/tmp/test_copy.sql <<EOF\rCOPY t FROM STDIN CSV;\r1,a\r\\.\r\rCOPY t FROM STDIN;\rEOF\r"
# Tab doesn't work nicely in cat<<EOF, so use echo to append the character with tab.
send "echo -e '2\\tb' >> /tmp/test_copy.sql\r"
eexpect ":/# "
send "$argv sql --insecure -f /tmp/test_copy.sql\r"
eexpect ":/# "
send "$argv sql --insecure -e 'SELECT * FROM t ORDER BY id'\r"
eexpect "1 | a"
eexpect "2 | b"
eexpect "(2 rows)"
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

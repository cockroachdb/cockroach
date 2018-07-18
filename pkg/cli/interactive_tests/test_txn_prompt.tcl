#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

send "$argv sql\r"
eexpect root@

###START tests prompt customization

start_test "Check that invalid prompt patterns cause an error."
send "\\set prompt1 %?\r"
eexpect "unrecognized format code in prompt"

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check that one can use % signs in the prompt."
send "\\set prompt1 abc%%def\r"
eexpect "abc%def"

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check that one can use all prompt customization keys."
send "SET database = 'defaultdb';\r"
eexpect "\nSET\r\n"
eexpect root@
send "\\set prompt1 %%M:%M:%%m:%m:%%>:%>:%%n:%n:%%/:%/:%%x:%x>\r"
eexpect %M::26257:%m::%>:26257:%n:root:%/:defaultdb:%x:

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check that one can use %M 'full host' key." 
send "\\set prompt1 FULLHOST:%M>\r"
eexpect FULLHOST::26257

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check that one can use %m 'host name' key." 
send "\\set prompt1 HOSTNAME:%m>\r"
eexpect HOSTNAME:

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check that one can use %> 'port number' key." 
send "\\set prompt1 PORT:%>>\r"
eexpect PORT:26257

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test


start_test "Check that one can use %n 'user name' key." 
send "\\set prompt1 USER:%n>\r"
eexpect USER:root

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test


start_test "Check that one can use %/ 'database' key." 
send "\\set prompt1 DATABASE:%/>\r"
eexpect DATABASE:defaultdb

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test


start_test "Check that one can use %x 'txn status' key." 
send "\\set prompt1 txnStatus:%x>\r"
eexpect txnStatus:

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check none define keys." 
send "\\set prompt1 #&@ \r"
eexpect #&@

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check continuePrompt working as expected."  
send "\\set prompt1 ### \r"
eexpect ###

send "SET database \r"
eexpect "\r\n -> "
send "defaultdb;\r" 
eexpect ###
# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test 



###END tests prompt customization


start_test "Check option to echo statements"
send "\\set echo\r"
send "select 1;\r"
eexpect "\n> select 1;\r\n"
eexpect root@
end_test

start_test "Check database prompt."
send "CREATE DATABASE IF NOT EXISTS testdb;\r"
eexpect "\nCREATE DATABASE\r\n"
eexpect root@
send "SET DATABASE = testdb;\r"
eexpect "\nSET\r\n"
eexpect root@
eexpect "/testdb>"
send "SET sql_safe_updates = false;\r"
eexpect "\nSET\r\n"
send "SET database = '';\r"
eexpect "\nSET\r\n"
eexpect root@
eexpect "/>"
send "SET database = 'defaultdb';\r"
eexpect "\nSET\r\n"
eexpect root@
end_test

start_test "Test that prompt becomes OPEN when txn is opened."
send "BEGIN;\r\r"

eexpect "\nBEGIN\r\n"
eexpect root@
eexpect "OPEN>"
end_test

start_test "Test that prompt becomes ERROR upon txn error."
send "select a;\r"
eexpect "pq: column \"a\" does not exist"
eexpect root@
eexpect "ERROR>"
end_test

start_test "Test that prompt becomes DONE after successful retry attempt."
send "ROLLBACK;\r"
eexpect "\nROLLBACK\r\n"
eexpect root@

send "BEGIN; SAVEPOINT cockroach_restart;\r\r"
eexpect SAVEPOINT
eexpect root@
send "SELECT 1;\r"
eexpect "1 row"
eexpect root@
send "RELEASE SAVEPOINT cockroach_restart;\r"
eexpect "\nCOMMIT\r\n"
eexpect root@
eexpect "DONE>"
end_test

start_test "Test that prompt becomes RETRY upon retry error."
send "COMMIT;\r"
eexpect root@

send "BEGIN; SAVEPOINT cockroach_restart;\r\r"
eexpect SAVEPOINT
eexpect root@
send "SELECT crdb_internal.force_retry('1s':::INTERVAL);\r"
eexpect "pq: restart transaction"
eexpect root@
eexpect "RETRY>"
end_test

start_test "Test that prompt reverts to OPEN at beginning of new attempt."
send "ROLLBACK TO SAVEPOINT cockroach_restart;\r"
eexpect ROLLBACK
eexpect root@
eexpect "OPEN>"
end_test

send "COMMIT;\r"
eexpect root@

start_test "Test that prompt becomes ??? upon server unreachable."
stop_server $argv

send "SELECT 1; SELECT 1;\r"
eexpect "connection lost"
eexpect root@
eexpect " \\?>"
end_test

# Terminate.
send "\\q\r"
eexpect ":/# "

send "exit\r"
eexpect eof


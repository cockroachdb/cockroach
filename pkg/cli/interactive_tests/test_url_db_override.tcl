#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

# This is run as an acceptance test to ensure that the code path
# that opens the SQL connection by URL is exercised.

system "$argv sql -e 'create database test'"
system "$argv user set test"

start_test "Check that the insecure flag can override the sslmode if not already in the URL."
# Use default, sslmode is secure
set ::env(COCKROACH_INSECURE) "false"

spawn $argv sql --url "postgresql://root@localhost:26257" -e "select 1"
eexpect "problem using security settings"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257" --insecure -e "select 1"
eexpect "1 row"
eexpect eof

set ::env(COCKROACH_INSECURE) "true"
end_test

start_test "Check that the insecure flag does not override the sslmode if already in the URL."
# Use default, sslmode is secure
set ::env(COCKROACH_INSECURE) "false"

spawn $argv sql --url "postgresql://test@localhost:26257?sslmode=verify-full" -e "select 1"
eexpect "password:"
send "\r"
eexpect "SSL is not enabled on the server"
eexpect eof

spawn $argv sql --url "postgresql://test@localhost:26257?sslmode=verify-full" --insecure -e "select 1"
eexpect "url already specifies SSL settings, ignoring"
eexpect "password:"
send "\r"
eexpect "SSL is not enabled on the server"
eexpect eof

set ::env(COCKROACH_INSECURE) "true"
end_test


start_test "Check that the database flag does not override the db if already set in the URL."
spawn $argv sql --url "postgresql://root@localhost:26257/system" --insecure -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "6,system"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257/system" --insecure --database test -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "url already specifies database, ignoring"
eexpect "l,db"
eexpect "6,system"
eexpect eof
end_test

start_test "Check that the database flag can set the db if missing from the URL."
# Use default, no db
spawn $argv sql --url "postgresql://root@localhost:26257" --insecure -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "0,"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257" --insecure --database test -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "4,test"
eexpect eof
end_test


start_test "Check that the database flag does not override the db if already set in the URL."
spawn $argv sql --url "postgresql://root@localhost:26257/system" --insecure -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "6,system"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257/system" --insecure --database test -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "url already specifies database, ignoring"
eexpect "l,db"
eexpect "6,system"
eexpect eof
end_test

start_test "Check that the user flag can set the user if missing from the URL."
# Use default, root
spawn $argv sql --url "postgresql://localhost:26257" --insecure -e "select length(@1) as l, @1 as u from \[show session_user\]" --format=csv
eexpect "l,u"
eexpect "4,root"
eexpect eof

spawn $argv sql --url "postgresql://localhost:26257" --insecure --user test -e "select length(@1) as l, @1 as u from \[show session_user\]" --format=csv
eexpect "l,u"
eexpect "4,test"
eexpect eof

start_test "Check that the user flag does not override the user if already set in the URL."
spawn $argv sql --url "postgresql://test@localhost:26257" --insecure -e "select length(@1) as l, @1 as u from \[show session_user\]" --format=csv
eexpect "l,u"
eexpect "4,test"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257" --insecure --user test -e "select length(@1) as l, @1 as u from \[show session_user\]" --format=csv
eexpect "url already specifies username, ignoring"
eexpect "l,u"
eexpect "4,root"
eexpect eof

end_test



stop_server $argv

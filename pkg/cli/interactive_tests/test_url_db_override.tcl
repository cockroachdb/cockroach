#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

# This is run as an acceptance test to ensure that the code path
# that opens the SQL connection by URL is exercised.

system "$argv sql -e 'create database test; create user test'"
set certs_dir "/certs"

start_test "Check that the SSL settings come from flags is URL does not set them already."
# Use default, sslmode is secure
set ::env(COCKROACH_INSECURE) "false"

spawn $argv sql --url "postgresql://test@localhost:26257" -e "select 1"
eexpect "cannot load certificates"
eexpect eof

spawn $argv sql --url "postgresql://test@localhost:26257" --insecure -e "select 1"
eexpect "1 row"
eexpect eof

set ::env(COCKROACH_INSECURE) "true"
end_test



start_test "Check that the insecure flag overrides the sslmode if URL is already set."
# Use default, sslmode is secure
set ::env(COCKROACH_INSECURE) "false"

spawn $argv sql --url "postgresql://test@localhost:26257?sslmode=verify-full" --certs-dir=$certs_dir -e "select 1"
eexpect "SSL is not enabled on the server"
eexpect eof

spawn $argv sql --url "postgresql://test@localhost:26257?sslmode=verify-full" --certs-dir=$certs_dir --insecure -e "select 1"
eexpect "1 row"
eexpect eof

set ::env(COCKROACH_INSECURE) "true"
end_test


start_test "Check that the database flag overrides the db if URL is already set."
spawn $argv sql --url "postgresql://root@localhost:26257/system?sslmode=disable"  -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "6,system"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257/system?sslmode=disable" --database test -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "4,test"
eexpect eof
end_test

start_test "Check that the database flag does override the database if none was present in the URL."
# Use empty path.
spawn $argv sql --url "postgresql://root@localhost:26257?sslmode=disable" --database system -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "6,system"
eexpect eof
# Use path = /
spawn $argv sql --url "postgresql://root@localhost:26257/?sslmode=disable" --database system -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "6,system"
eexpect eof

end_test

start_test "Check that the host flag overrides the host if URL is already set."
spawn $argv sql --url "postgresql://root@localhost:26257?sslmode=disable" --host nonexistent.invalid -e "select 1"
eexpect "cannot dial server"
eexpect eof
end_test

set ::env(COCKROACH_INSECURE) "false"

start_test "Check that the user flag  override the user if URL is already set."
spawn $argv sql --url "postgresql://test@localhost:26257?sslmode=disable" -e "select length(@1) as l, @1 as u from \[show session_user\]" --format=csv
eexpect "l,u"
eexpect "4,test"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257?sslmode=disable" --user test -e "select length(@1) as l, @1 as u from \[show session_user\]" --format=csv
eexpect "l,u"
eexpect "4,test"
eexpect eof
end_test


stop_server $argv

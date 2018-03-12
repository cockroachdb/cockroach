#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

# This is run as an acceptance test to ensure that the code path
# that opens the SQL connection by URL is exercised.

system "$argv sql -e 'create database test'"

start_test "Check that the database flag can set the db if missing from the URL."
spawn $argv sql --url "postgresql://root@localhost:26257?sslmode=disable" -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "0,"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257?sslmode=disable" --database test -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "4,test"
eexpect eof
end_test

start_test "Check that the database flag can ovrride the db if already set in the URL."
spawn $argv sql --url "postgresql://root@localhost:26257/system?sslmode=disable" -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "6,system"
eexpect eof

spawn $argv sql --url "postgresql://root@localhost:26257/system?sslmode=disable" --database test -e "select length(@1) as l, @1 as db from \[show database\]" --format=csv
eexpect "l,db"
eexpect "4,test"
eexpect eof
end_test


stop_server $argv

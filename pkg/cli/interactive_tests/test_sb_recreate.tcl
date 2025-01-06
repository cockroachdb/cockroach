#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

proc file_exists {filepath} {
  if {! [ file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
}

start_test "Ensure that 'debug sb recreate' works with cross-DB references and special characters"

spawn $argv demo --no-line-editor --empty --log-dir=logs
eexpect "defaultdb>"

send "CREATE DATABASE \"db.table\";\r"
send "USE \"db.table\";\r"
send "CREATE TYPE udt AS ENUM ('a', 'b');\r"
send "CREATE SCHEMA \"sc.t\";\r"
send "CREATE TABLE \"sc.t\".t (k INT PRIMARY KEY, e udt);\r"

send "CREATE DATABASE \" db ' view \";\r"
send "USE \" db ' view \";\r"
send "CREATE TABLE t (k INT PRIMARY KEY);\r"
send "CREATE SCHEMA \"sc ' view\";\r"
send "CREATE VIEW \"sc ' view\".v AS SELECT * FROM t;\r"

send "RESET database;\r"
send "CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS 'SELECT 1';\r"
eexpect "defaultdb>"

send "EXPLAIN ANALYZE (DEBUG) SELECT *, f() FROM \"db.table\".\"sc.t\".t, \" db ' view \".\"sc ' view\".v;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set id1 $expect_out(1,string)
}

send "\\statement-diag download $id1\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$id1.zip"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle"
system "$python $pyfile stmt-bundle-$id1.zip bundle"

spawn $argv debug sb recreate bundle
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

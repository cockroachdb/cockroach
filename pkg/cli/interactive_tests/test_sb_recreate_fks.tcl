#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

proc file_exists {filepath} {
  if {! [ file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
}

start_test "Ensure that 'debug sb recreate' works with FK references"

spawn $argv demo --no-line-editor --empty --log-dir=logs
eexpect "defaultdb>"

send "CREATE TABLE parent (pk INT PRIMARY KEY, v INT);\r"
send "CREATE TABLE child (pk INT PRIMARY KEY, fk INT REFERENCES parent(pk));\r"
send "CREATE TABLE grandchild (pk INT PRIMARY KEY, fk INT REFERENCES child(pk));\r"
eexpect "defaultdb>"


# First, perform writes against each table. These bundles should include FK
# references.


send "EXPLAIN ANALYZE (DEBUG) INSERT INTO parent VALUES (1, 1);\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set w1 $expect_out(1,string)
}

send "\\statement-diag download $w1\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$w1.zip"

send "EXPLAIN ANALYZE (DEBUG) INSERT INTO child VALUES (1, 1);\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set w2 $expect_out(1,string)
}

send "\\statement-diag download $w2\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$w2.zip"

send "EXPLAIN ANALYZE (DEBUG) INSERT INTO grandchild VALUES (1, 1);\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set w3 $expect_out(1,string)
}

send "\\statement-diag download $w3\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$w3.zip"


# Now perform reads against each table. These bundles shouldn't include FK
# references yet must be recreatable.


send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set r1 $expect_out(1,string)
}

send "\\statement-diag download $r1\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$r1.zip"

send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM child;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set r2 $expect_out(1,string)
}

send "\\statement-diag download $r2\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$r2.zip"

send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM grandchild;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set r3 $expect_out(1,string)
}

send "\\statement-diag download $r3\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$r3.zip"

send_eof
eexpect eof


# Recreate each bundle.


set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle1"
system "$python $pyfile stmt-bundle-$w1.zip bundle1"

spawn $argv debug sb recreate bundle1
eexpect "Statement was:"
eexpect "INSERT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle2"
system "$python $pyfile stmt-bundle-$w2.zip bundle2"

spawn $argv debug sb recreate bundle2
eexpect "Statement was:"
eexpect "INSERT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle3"
system "$python $pyfile stmt-bundle-$w3.zip bundle3"

spawn $argv debug sb recreate bundle3
eexpect "Statement was:"
eexpect "INSERT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle4"
system "$python $pyfile stmt-bundle-$r1.zip bundle4"

spawn $argv debug sb recreate bundle4
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle5"
system "$python $pyfile stmt-bundle-$r2.zip bundle5"

spawn $argv debug sb recreate bundle5
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle6"
system "$python $pyfile stmt-bundle-$r3.zip bundle6"

spawn $argv debug sb recreate bundle6
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

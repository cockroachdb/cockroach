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
send "CREATE TABLE child (pk INT PRIMARY KEY, fk INT REFERENCES parent(pk) ON DELETE CASCADE ON UPDATE CASCADE);\r"
send "CREATE TABLE grandchild (pk INT PRIMARY KEY, fk INT REFERENCES child(pk) ON DELETE CASCADE ON UPDATE CASCADE);\r"
send "CREATE TABLE greatgrandchild (pk INT PRIMARY KEY, fk INT REFERENCES grandchild(pk));\r"
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


# Now perform reads against each table as well as some combination of tables.
# These bundles might include some FK references.


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

send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, child;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set r4 $expect_out(1,string)
}

send "\\statement-diag download $r4\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$r4.zip"

send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, child, grandchild;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set r5 $expect_out(1,string)
}

send "\\statement-diag download $r5\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$r5.zip"

send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, grandchild;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set r6 $expect_out(1,string)
}

send "\\statement-diag download $r6\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$r6.zip"


# Now add a FK cycle and collect another bundle.

send "ALTER TABLE parent ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES child(pk) ON DELETE CASCADE ON UPDATE CASCADE;\r"
send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, child;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set r7 $expect_out(1,string)
}

send "\\statement-diag download $r7\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$r7.zip"


# Now perform some writes that result in CASCADEs.

# This bundle should only pull in 'child'.
send "EXPLAIN ANALYZE (DEBUG) UPDATE parent SET pk = pk + 1 WHERE true;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set w4 $expect_out(1,string)
}

send "\\statement-diag download $w4\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$w4.zip"

# This bundle will pull in all tables.
send "EXPLAIN ANALYZE (DEBUG) UPDATE child SET pk = pk + 1 WHERE true;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set w5 $expect_out(1,string)
}

send "\\statement-diag download $w5\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$w5.zip"

# This bundle will pull in all tables too.
send "EXPLAIN ANALYZE (DEBUG) DELETE FROM parent WHERE true;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set w6 $expect_out(1,string)
}

send "\\statement-diag download $w6\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$w6.zip"

send_eof
eexpect eof


# Recreate each bundle.


set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_w1"
system "$python $pyfile stmt-bundle-$w1.zip bundle_w1"

spawn $argv debug sb recreate bundle_w1
eexpect "Statement was:"
eexpect "INSERT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_w2"
system "$python $pyfile stmt-bundle-$w2.zip bundle_w2"

spawn $argv debug sb recreate bundle_w2
eexpect "Statement was:"
eexpect "INSERT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_w3"
system "$python $pyfile stmt-bundle-$w3.zip bundle_w3"

spawn $argv debug sb recreate bundle_w3
eexpect "Statement was:"
eexpect "INSERT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_r1"
system "$python $pyfile stmt-bundle-$r1.zip bundle_r1"

spawn $argv debug sb recreate bundle_r1
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_r2"
system "$python $pyfile stmt-bundle-$r2.zip bundle_r2"

spawn $argv debug sb recreate bundle_r2
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_r3"
system "$python $pyfile stmt-bundle-$r3.zip bundle_r3"

spawn $argv debug sb recreate bundle_r3
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_r4"
system "$python $pyfile stmt-bundle-$r4.zip bundle_r4"

spawn $argv debug sb recreate bundle_r4
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_r5"
system "$python $pyfile stmt-bundle-$r5.zip bundle_r5"

spawn $argv debug sb recreate bundle_r5
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_r6"
system "$python $pyfile stmt-bundle-$r6.zip bundle_r6"

spawn $argv debug sb recreate bundle_r6
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_r7"
system "$python $pyfile stmt-bundle-$r7.zip bundle_r7"

spawn $argv debug sb recreate bundle_r7
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_w4"
system "$python $pyfile stmt-bundle-$w4.zip bundle_w4"

spawn $argv debug sb recreate bundle_w4
eexpect "Statement was:"
eexpect "UPDATE"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_w5"
system "$python $pyfile stmt-bundle-$w5.zip bundle_w5"

spawn $argv debug sb recreate bundle_w5
eexpect "Statement was:"
eexpect "UPDATE"
eexpect "defaultdb>"

send_eof
eexpect eof

set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle_w6"
system "$python $pyfile stmt-bundle-$w6.zip bundle_w6"

spawn $argv debug sb recreate bundle_w6
eexpect "Statement was:"
eexpect "DELETE"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

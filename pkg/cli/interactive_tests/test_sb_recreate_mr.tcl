#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

proc file_exists {filepath} {
  if {! [ file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
}

start_test "Ensure that 'debug sb recreate' works for multi-region tables"

# Start with more regions than are actually used in the bundle.
spawn $argv demo --no-line-editor --empty --nodes 7 --demo-locality=region=us-east1,az=1:region=us-west1,az=1:region=us-central1,az=1:region=eu-east1,az=2:region=eu-west1,az=2:region=eu-central1,az=2:region=ap-east1,az=3 --log-dir=logs
eexpect "defaultdb>"

send "CREATE DATABASE us_mr PRIMARY REGION \"us-east1\" REGIONS = \"us-east1\", \"us-west1\", \"us-central1\";\r"
send "CREATE TYPE us_mr.public.enum1 AS ENUM ('a', 'b', 'c');\r"
send "CREATE TABLE us_mr.public.t (k INT PRIMARY KEY, e us_mr.public.enum1) LOCALITY REGIONAL BY ROW;\r"
send "CREATE DATABASE eu_mr PRIMARY REGION \"eu-central1\" REGIONS = \"eu-central1\", \"eu-east1\", \"eu-west1\";\r"
send "CREATE TYPE eu_mr.public.enum1 AS ENUM ('a', 'b', 'c');\r"
send "CREATE TABLE eu_mr.public.t (k INT PRIMARY KEY, e eu_mr.public.enum1) LOCALITY REGIONAL BY ROW;\r"
send "USE us_mr;\r"
eexpect "us_mr>"

send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM t, eu_mr.public.t;\r"
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
eexpect "Started 6 nodes with regions"
eexpect "Statement was:"
eexpect "SELECT"
eexpect "us_mr>"

send_eof
eexpect eof

end_test

#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# The following tests want to access the licensing server.
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "false"

start_test "Expect partitioning succeeds"
# test that partitioning works if a license could be acquired
spawn $argv demo --geo-partitioned-replicas

# wait for the shell to start up
expect {
  # We can't test a particular error string, because a license acquisition
  # can fail in various ways (i.e. not just a timeout).
  # So instead we use the specific error message shouted by `cockroach demo` when license
  # acquisition fails.
  "license acquisition was unsuccessful" {
      # The license server is unreachable. There's not much we can test here.
      # Simply ignore the test.
      report "License server could not be reached - skipping with no error"
      exit 0
  }
  "movr>" {}
}

send "SELECT count(*) AS NRPARTS FROM \[SHOW PARTITIONS FROM DATABASE movr\];\r"
eexpect "nrparts"
eexpect "24"
eexpect "(1 row)"

send "SHOW PARTITIONS FROM TABLE vehicles;\r"

# Verify the partitions are as we expect
send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='us_west';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='us_west' AND partition_value='(''seattle''), (''san francisco''), (''los angeles'')';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='us_west' AND zone_config='constraints = ''\[+region=us-west1\]''';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='us_east';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='us_east' AND partition_value='(''new york''), (''boston''), (''washington dc'')';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='us_east' AND zone_config='constraints = ''\[+region=us-east1\]''';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='europe_west';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='europe_west' AND partition_value='(''amsterdam''), (''paris''), (''rome'')';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\] WHERE partition_name='europe_west' AND zone_config='constraints = ''\[+region=europe-west1\]''';\r"
eexpect "8"
eexpect "(1 row)"
eexpect "movr>"

interrupt
eexpect eof
end_test


start_test "Expect an error if geo-partitioning is requested and a license cannot be acquired"
set env(COCKROACH_DEMO_LICENSE_URL) "https://127.0.0.1:9999/"
spawn $argv demo --geo-partitioned-replicas
eexpect "error while contacting licensing server"
eexpect "dial tcp"
eexpect "ERROR: license acquisition was unsuccessful"
eexpect eof
end_test

start_test "Expect an error if geo-partitioning is requested and license acquisition is disabled"

# set the proper environment variable
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "true"
spawn $argv demo --geo-partitioned-replicas
# expect a failure
eexpect ERROR:
eexpect "enterprise features are needed for this demo"
# clean up after the test
eexpect eof

end_test

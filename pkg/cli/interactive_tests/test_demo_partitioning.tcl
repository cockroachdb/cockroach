#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Expect partitioning succeeds"
# test that partitioning works if a license could be acquired
spawn $argv demo --geo-partitioned-replicas

# wait for the shell to start up
eexpect "movr>"

# send multiple "SHOW PARTITIONS" requests to the DB as partitioning is happen asynchronously.
for {set i 0} {$i < 10} {incr i} {
  send "SELECT count(*) FROM \[SHOW PARTITIONS FROM DATABASE movr\];\r"
  sleep 1
}

# The number of partitions across the MovR database we expect is 24.
eexpect "24"
eexpect "(1 row)"
eexpect "movr>"

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

# set the proper environment variable
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "true"
spawn $argv demo --geo-partitioned-replicas
# expect a failure
eexpect "Error: license acquisition was unsuccessful. Enterprise features are needed to partition data"
# clean up after the test
interrupt
eexpect eof
end_test


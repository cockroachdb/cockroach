#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set a larger timeout as partitioning can be slow.
set timeout 300

# The following tests want to access the licensing server.
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "false"

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

start_test "Expect partitioning succeeds"
# test that partitioning works if a license could be acquired
send "$argv demo --no-line-editor --multitenant=false --geo-partitioned-replicas --log-dir=logs \r"

# wait for the shell to start up
eexpect "Partitioning the demo database"
eexpect "movr>"

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

send_eof
eexpect $prompt
end_test

start_test "test multi-region setup"
send "$argv demo movr --no-line-editor --geo-partitioned-replicas --multi-region --log-dir=logs \r"
eexpect "Partitioning the demo database"
eexpect "movr>"

send "select table_name, locality from \[show tables\] order by 1;\r"
eexpect "          table_name         |    locality"
eexpect "  promo_codes                | GLOBAL"
eexpect "  rides                      | REGIONAL BY ROW"
eexpect "  user_promo_codes           | REGIONAL BY ROW"
eexpect "  users                      | REGIONAL BY ROW"
eexpect "  vehicle_location_histories | REGIONAL BY ROW"
eexpect "  vehicles                   | REGIONAL BY ROW"
eexpect "movr>"

send "select survival_goal from \[show databases\] where database_name = 'movr';\r"
eexpect "  survival_goal"
eexpect "zone"
eexpect "movr>"

send_eof
eexpect $prompt

send "$argv demo movr --no-line-editor --geo-partitioned-replicas --multi-region --survive=region --log-dir=logs \r"
eexpect "Partitioning the demo database"
eexpect "movr>"

send "select survival_goal from \[show databases\] where database_name = 'movr';\r"
eexpect "  survival_goal"
eexpect "region"
eexpect "movr>"

send_eof
eexpect $prompt

end_test

# Test interaction of -e and license acquisition.
start_test "Ensure we can run licensed commands with -e"
send "$argv demo -e \"ALTER TABLE users PARTITION BY LIST (city) (PARTITION p1 VALUES IN ('new york'))\" --log-dir=logs \r"
eexpect "ALTER TABLE"
eexpect $prompt
end_test

start_test "Ensure that we can run licensed commands with -e when license acquisition is disabled"
send "$argv demo --disable-demo-license -e \"ALTER TABLE users PARTITION BY LIST (city) (PARTITION p1 VALUES IN ('new york'))\" --log-dir=logs \r"
eexpect "ALTER TABLE"
eexpect $prompt
end_test

start_test "Expect an error if geo-partitioning is requested and license acquisition is disabled"
# set the proper environment variable
send "$argv demo --no-line-editor --geo-partitioned-replicas --disable-demo-license --log-dir=logs \r"
# expect a failure
eexpect "enterprise features are needed for this demo"
eexpect $prompt
end_test

# clean up after the test
send_eof
eexpect eof
end_test

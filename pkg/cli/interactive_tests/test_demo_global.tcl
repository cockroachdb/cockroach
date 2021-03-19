#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set a larger timeout since we are talking to every node with a delay.
set timeout 90

start_test "Check --global flag runs as expected"

# Start a demo with --global set
spawn $argv demo --empty --nodes 9 --global

# Ensure db is defaultdb.
eexpect "defaultdb>"

# Create some tables.
send "CREATE TABLE europewest (id INT);\r"
eexpect "defaultdb>"
send "ALTER TABLE europewest CONFIGURE ZONE USING
    num_replicas = 3,
    num_voters = 3,
    gc.ttlseconds = 5,
    constraints = '{+region=europe-west1: 3}',
    voter_constraints = '\[+region=europe-west1\]',
    lease_preferences = '\[\[+region=europe-west1\]\]';\r"
eexpect "defaultdb>"

send "CREATE TABLE uswest (id INT);\r"
eexpect "defaultdb>"
send "ALTER TABLE uswest CONFIGURE ZONE
    USING num_replicas = 3,
    num_voters = 3,
    gc.ttlseconds = 5,
    constraints = '{+region=us-west1: 3}',
    voter_constraints = '\[+region=us-west1\]',
    lease_preferences = '\[\[+region=us-west1\]\]';\r"
eexpect "defaultdb>"

proc run_query {query expected_min_time} {
  set before_time [clock milliseconds]
  send "$query\r"
  eexpect "defaultdb>"
  set time_taken [expr [clock milliseconds] - $before_time]
  return [expr $time_taken > $expected_min_time]
}

proc retry_run_query {query expected_min_time} {
  # We need to retry this query as replication or leaseholders may not be
  # in effect after CONFIGURE ZONE.
  for {set i 0} {$i < 10} {incr i} {
    if [ run_query $query $expected_min_time ] {
      # Run query one more to ensure it was not a flake.
      report "Query ran successfully in expected time once, try once more to confirm it was not a fluke"
      if [ run_query $query $expected_min_time ] {
        return
      }
    }
    set timeout_ms [expr {$i * $i * 500}]
    report "BACKING OFF AND RETRYING"
    after $timeout_ms
  }
  report "FAILED TO RUN $query WITHIN $expected_min_time ms"
  exit 1
}

retry_run_query "INSERT INTO europewest VALUES (1);" 64
retry_run_query "INSERT INTO uswest VALUES (1);" 66

interrupt
eexpect eof
end_test

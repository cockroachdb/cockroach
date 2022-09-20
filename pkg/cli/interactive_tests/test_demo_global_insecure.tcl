#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set a larger timeout since we are talking to every node with a delay.
set timeout 90

start_test "Check --global flag runs as expected"

# Start a demo with --global set
# TODO(ajstorm): Disable multitenancy until #76305 is resolved.
spawn $argv demo --no-line-editor --no-example-database --nodes 9 --global --multitenant=false --insecure

# Ensure db is defaultdb.
eexpect "defaultdb>"

# Ensure regions display correctly.
send "SELECT region, zones FROM \[SHOW REGIONS FROM CLUSTER\] ORDER BY region;\r"
eexpect "  europe-west1 | {b,c,d}"
eexpect "  us-east1     | {b,c,d}"
eexpect "  us-west1     | {a,b,c}"

# Test we cannot add or restart nodes.
send "\\demo add region=europe-west1\r"
eexpect "adding nodes is not supported in --global configurations"
eexpect "defaultdb>"

send "\\demo shutdown 3\r"
eexpect "shutting down nodes is not supported in --global configurations"
eexpect "defaultdb>"

# Test that the correct number of node-node pairs have latencies higher
# than 60ms, indicating that simulated latencies are working.
set timeout 1
set stmt "SELECT count\(\*\) FROM \( SELECT node_id, json_object_keys\(activity\) AS other_id, activity FROM crdb_internal.kv_node_status\) WHERE node_id != other_id::INT8 and \(\(\(activity-\>other_id-\>\>'latency'\)::FLOAT8) \/ 1e6\) > 60;\r"
send $stmt
expect {
    "     54" {
        puts "\rfound correct number of node pairs"
    }
    timeout {
        puts "\rdid not see correct number of node pairs - retrying"
        sleep 2
        send $stmt
        exp_continue
    }
}

# The eof takes longer with simulated latencies so we may need to
# retry.
send_eof
expect {
    eof {
        end_test
    }
    timeout {
        puts "\rcontinuing to wait for eof"
        sleep 1
        exp_continue
    }
}

end_test

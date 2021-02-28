#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
set shell1_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

set ::env(COCKROACH_INSECURE) "false"

system "hostname >hostname.txt"

start_test "Check that the connect init command can generate single-node credentials"
# Run connect init. We are careful to preserve the generated files into the logs sub-directory
# so that the artifacts remain for investigation if the command fail.
# The reason why we do not use --certs-dir=logs directly is that the log directory
# makes its contents world-readable, and crdb asserts that cert / key files
# are not world-readable.
send "$argv connect init --single-node --listen-addr=`cat hostname.txt` --http-addr=`cat hostname.txt` --certs-dir=certs/sn; cp -a certs logs/\r"
eexpect "generating cert bundle"
eexpect "cert files generated"
eexpect ":/# "
end_test

start_test "Check that we can start a secure server with that"
system "$argv start-single-node --listen-addr=`cat hostname.txt` --certs-dir=certs/sn --pid-file=server_pid -s=path=logs/db --background >>logs/expect-cmd.log 2>&1"
end_test

# NB: we will be able to remove the manual generation of root certs
# some time in the future.
system "$argv cert create-client root --ca-key=certs/sn/ca-client.key --certs-dir=certs/sn"

start_test "Check we can connect init a SQL client with that"
system "$argv sql --certs-dir=certs/sn --host=`cat hostname.txt` -e 'select 1'"
end_test

# Stop the server we started above.
stop_server $argv

spawn /bin/bash
set shell2_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

system "mkdir -p logs/n1 logs/n2"

start_test "Check that the connect init command can generate certs for two nodes."
set spawn_id $shell1_spawn_id
send "$argv connect init --num-expected-initial-nodes 2 --init-token=abc --listen-addr=`cat hostname.txt`:26257 --http-addr=`cat hostname.txt`:8080 --join=`cat hostname.txt`:26258 --certs-dir=certs/n1 --log='file-defaults: {dir: logs/n1}\r"
send "sinks: {stderr: {filter: NONE}}'\r"
eexpect "waiting for handshake"

set spawn_id $shell2_spawn_id
send "$argv connect init --num-expected-initial-nodes 2 --init-token=abc --listen-addr=`cat hostname.txt`:26258 --http-addr=`cat hostname.txt`:8081 --join=`cat hostname.txt`:26257 --certs-dir=certs/n2 --log='file-defaults: {dir: logs/n2}\r"
send "sinks: {stderr: {filter: NONE}}'\r"
eexpect "waiting for handshake"
eexpect "trusted peer"
eexpect "cert bundle"
eexpect "cert files generated in: certs/n2"
eexpect ":/# "

set spawn_id $shell1_spawn_id
eexpect "trusted peer"
eexpect "cert bundle"
eexpect "cert files generated in: certs/n1"
eexpect ":/# "
end_test

# Keep the generated certs for both nodes to the artifacts directory.
system "cp -a certs logs/"

# NB: we will be able to remove the manual generation of root certs
# some time in the future.
system "$argv cert create-client root --ca-key=certs/n1/ca-client.key --certs-dir=certs/n1"
system "$argv cert create-client root --ca-key=certs/n2/ca-client.key --certs-dir=certs/n2"

start_test "Check that we can start two servers using the newly minted certs."
send "$argv start --listen-addr=`cat hostname.txt`:26257 --http-addr=`cat hostname.txt`:8080 --join=`cat hostname.txt`:26258 --certs-dir=certs/n1 --store=logs/db1 --pid-file=server_pid1 --vmodule='*=1'\r"
eexpect "initial startup completed"

set spawn_id $shell2_spawn_id
send "$argv start --listen-addr=`cat hostname.txt`:26258 --http-addr=`cat hostname.txt`:8081 --join=`cat hostname.txt`:26257 --certs-dir=certs/n2 --store=logs/db2 --pid-file=server_pid2 --vmodule='*=1'\r"
eexpect "initial startup completed"

# Now initialize the cluster to trigger generation of the node IDs.
system "$argv init --certs-dir=certs/n1 --host `cat hostname.txt`"

# Now expect the startup messages on both process outputs.
eexpect "CockroachDB node starting"
set spawn_id $shell1_spawn_id
eexpect "CockroachDB node starting"
end_test

start_test "Check we can connect init a SQL client to the newly initialized two nodes"
system "$argv sql --certs-dir=certs/n1 --host=`cat hostname.txt`:26257 -e 'select 1'"
system "$argv sql --certs-dir=certs/n2 --host=`cat hostname.txt`:26258 -e 'select 1'"
end_test

# Stop the servers. We do not care about graceful shutdown here since we are not
# using the server files again beyond this point.
system "kill -9 `cat server_pid1 server_pid2`"


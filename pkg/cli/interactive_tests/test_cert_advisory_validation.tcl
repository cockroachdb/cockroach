#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

# create some cert without an IP address in there.
set db_dir "logs/db"
set certs_dir "logs/my-safe-directory"
send "mkdir -p $certs_dir\r"
eexpect $prompt

send "$argv cert create-ca --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt
send "$argv cert create-node localhost --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt

start_test "Check that the server reports a warning if attempting to advertise an IP address not in cert."
send "$argv start-single-node --store=$db_dir --certs-dir=$certs_dir --advertise-addr=127.0.0.1\r"
eexpect "advertise address"
eexpect "127.0.0.1"
eexpect "not in node certificate"
eexpect "node starting"
interrupt
eexpect "interrupted"
eexpect $prompt
end_test

start_test "Check that the server reports no warning if the avertise addr is in the cert."
send "$argv start-single-node --store=$db_dir --certs-dir=$certs_dir --advertise-addr=localhost\r"
expect {
  "not in node certificate" {
     report "unexpected warning"
     exit 1
  }
  "node starting" {}
}
interrupt
eexpect "interrupted"
expect $prompt
end_test

send "rm -f $certs_dir/node.*\r"
eexpect $prompt
send "COCKROACH_CERT_NODE_USER=foo.bar $argv cert create-node localhost --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt

start_test "Check that the server reports an error if the node cert does not contain a node principal."
send "$argv start-single-node --store=$db_dir --certs-dir=$certs_dir --advertise-addr=localhost\r"
eexpect "cannot load certificates"
expect $prompt
end_test

start_test "Check that the cert principal map can allow the use of non-standard cert principal."
send "$argv start-single-node --store=$db_dir --certs-dir=$certs_dir --cert-principal-map=foo.bar:node --advertise-addr=localhost\r"
eexpect "node starting"
interrupt
eexpect "interrupted"
expect $prompt
end_test

start_test "Check that the cert principal map can allow the use of a SAN principal."
send "$argv start-single-node --store=$db_dir --certs-dir=$certs_dir --cert-principal-map=localhost:node --advertise-addr=localhost\r"
eexpect "node starting"
interrupt
eexpect "interrupted"
expect $prompt
end_test

start_test "Check that 'cert list' can utilize cert principal map."
send "$argv cert list --certs-dir=$certs_dir --cert-principal-map=foo.bar:node\r"
eexpect "Certificate directory:"
expect $prompt
end_test

start_test "Check that 'cert create-client' can utilize cert principal map."
send "$argv cert create-client root.crdb.io --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key --cert-principal-map=foo.bar:node\r"
eexpect $prompt
send "mv $certs_dir/client.root.crdb.io.crt $certs_dir/client.root.crt; mv $certs_dir/client.root.crdb.io.key $certs_dir/client.root.key\r"
eexpect $prompt
end_test

start_test "Check that the client commands can use cert principal map."
system "$argv start-single-node --store=$db_dir --certs-dir=$certs_dir --cert-principal-map=foo.bar:node,root.crdb.io:root --advertise-addr=localhost --background >>expect-cmd.log 2>&1"
send "$argv sql --certs-dir=$certs_dir --cert-principal-map=foo.bar:node,root.crdb.io:root -e \"select 'hello'\"\r"
eexpect "hello"
expect $prompt
send "$argv node ls --certs-dir=$certs_dir --cert-principal-map=foo.bar:node,root.crdb.io:root\r"
eexpect "1 row"
expect $prompt
send "$argv quit --certs-dir=$certs_dir --cert-principal-map=foo.bar:node,root.crdb.io:root\r"
eexpect "ok"
expect $prompt
end_test

#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

# create some cert without an IP address in there.
set db_dir "logs/db"
set certs_dir "my-safe-directory"
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

start_test "Check that the server reports no warning if the advertise addr is in the cert."
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

send "rm -f $certs_dir/node.*\r"
eexpect $prompt
send "$argv cert create-node localhost --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt
send "$argv cert create-client foo --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt
send "$argv start-single-node --host=localhost --socket-dir=. --store=$db_dir --certs-dir=$certs_dir --cert-principal-map=foo:root --pid-file=server_pid --background\r"
eexpect $prompt
start_test "Check that the cert principal map can authenticate root user using non-db user cert CN."
send "$argv sql --certs-dir=$certs_dir --url=\"postgresql://root@localhost:26257?sslcert=$certs_dir/client.foo.crt&sslkey=$certs_dir/client.foo.key\" -e 'select 1'\r";
eexpect "(1 row)"
eexpect $prompt
end_test
stop_server $argv

send "$argv cert create-client root --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt
send "$argv start-single-node --host=localhost --socket-dir=. --store=$db_dir --certs-dir=$certs_dir --cert-principal-map=foo:bar --pid-file=server_pid --background\r"
eexpect $prompt
send "$argv sql --certs-dir=$certs_dir -e 'create user bar'\r"
eexpect $prompt
start_test "Check that cert auth fails when cert-principal-map and HBAconf(name-remapping) setting are both set for same db user."
set id_map_stmt "SET CLUSTER SETTING server.identity_map.configuration='crdb foo bar'"
set hba_conf_stmt "SET CLUSTER SETTING server.host_based_authentication.configuration='hostssl all bar all cert map=crdb'"
send "$argv sql --certs-dir=$certs_dir --user=root -e \"$id_map_stmt\" \r"
send "$argv sql --certs-dir=$certs_dir --user=root -e \"$hba_conf_stmt\" \r"
set auth_url "postgresql://bar@localhost:26257?sslcert=$certs_dir/client.foo.crt&sslkey=$certs_dir/client.foo.key"
send "$argv sql --certs-dir=$certs_dir --url=\"$auth_url\" -e 'select 1'\r";
eexpect "ERROR: certificate authentication failed for user \"foo\""
end_test
stop_server $argv

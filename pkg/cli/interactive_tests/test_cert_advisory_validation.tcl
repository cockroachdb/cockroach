#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

# create some cert without an IP address in there.
set certs_dir "./my-safe-directory"
send "mkdir -p $certs_dir\r"
eexpect $prompt

send "$argv cert create-ca --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt
send "$argv cert create-node localhost --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt

start_test "Check that the server reports a warning if attempting to advertise an IP address not in cert."
send "$argv start --certs-dir=$certs_dir --advertise-addr=127.0.0.1\r"
eexpect "advertise address"
eexpect "127.0.0.1"
eexpect "not in node certificate"
eexpect "node starting"
interrupt
eexpect "interrupted"
eexpect $prompt
end_test

start_test "Check that the server reports no warning if the avertise addr is in th cert."
send "$argv start --certs-dir=$certs_dir --advertise-addr=localhost\r"
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

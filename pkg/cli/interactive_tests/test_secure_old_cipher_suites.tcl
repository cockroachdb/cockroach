#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"

proc start_server {argv certs_dir extra} {
    report "BEGIN START SECURE SERVER"
    system "$argv start-single-node --host=localhost --socket-dir=. --certs-dir=$certs_dir --pid-file=server_pid -s=path=logs/db --background $extra >>expect-cmd.log 2>&1;
            $argv sql --certs-dir=$certs_dir -e 'select 1'"
    report "END START SECURE SERVER"
}

proc expect_exit_status {expected} {
    set status [lindex [wait] 3]
    if {$status != $expected} {
        report "unexpected exit status $status"
        exit 1
    }
}

# Create an openssl CA for client certs.
file copy [file join [file dirname $argv0] "ocsp_ca.cnf"] "ca.cnf"

report "GENERATING CA KEY"
system "openssl genrsa -out ca.key"
report "GENERATING CA CERT"
system "openssl req -new -x509 -config ca.cnf -key ca.key -out ca.crt -days 365 -batch"
system "touch index.txt; echo '01' > serial.txt"
# $certs_dir already contains a root cert signed by the node CA. Add
# our client CA to it instead of replacing ca.crt. This is also
# important because this root cert doesn't have the OCSP fields set,
# so we can still use it while we're testing OCSP errors.
system "cat ca.crt $certs_dir/ca.crt > $certs_dir/ca-client.crt"

start_server $argv $certs_dir ""

# Verify that old cipher suites are not enabled by default
start_test "NO OLD CIPHER SUITES"
system "go run test-ciphersuites/main.go --certs-dir=$certs_dir"
expect_exit_status 1
end_test

stop_server $argv

set ::env(COCKROACH_TLS_ENABLE_OLD_CIPHER_SUITES) "true"

# start the server with the TLS_ENABLE_OLD_CIPHER_SUITES option set to true.
start_server $argv $certs_dir ""

# Verify that old cipher suite is enabled
start_test "YES OLD CIPHER SUITES"
system "go run test-ciphersuites/main.go --certs-dir=$certs_dir"
end_test

stop_server $argv

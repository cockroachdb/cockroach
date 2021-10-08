#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"

proc start_secure_server {argv certs_dir extra} {
    report "BEGIN START SECURE SERVER"
    system "$argv start-single-node --host=localhost --socket-dir=. --certs-dir=$certs_dir --pid-file=server_pid -s=path=logs/db --background $extra >>expect-cmd.log 2>&1;
            $argv sql --certs-dir=$certs_dir -e 'select 1'"
    report "END START SECURE SERVER"
}

proc stop_secure_server {argv certs_dir} {
    report "BEGIN STOP SECURE SERVER"
    system "$argv quit --certs-dir=$certs_dir"
    report "END STOP SECURE SERVER"
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

proc create_user_cert {argv certs_dir name} {
    report "GENERATING CERT FOR USER $name"
    system "openssl genrsa -out $certs_dir/client.$name.key"
    system "openssl req -new -key $certs_dir/client.$name.key -out client.$name.csr -batch -subj /O=Cockroach/CN=$name"
    system "openssl ca -config ca.cnf -keyfile ca.key -cert ca.crt -policy signing_policy -extensions signing_client_req -out $certs_dir/client.$name.crt -outdir . -in client.$name.csr -batch"
    # Uncomment the next line to see more details about the generated cert
    #system "openssl x509 -in $certs_dir/client.$name.crt -text"
    system "$argv sql --certs-dir=$certs_dir -e 'create user $name'"
    system "$argv sql --certs-dir=$certs_dir --user=$name -e 'select 1'"
}

start_secure_server $argv $certs_dir ""

# Create users and make sure they can each log in.
create_user_cert $argv $certs_dir goofus
create_user_cert $argv $certs_dir gallant

report "REVOKING CERTIFICATE"
system "openssl ca -config ca.cnf -keyfile ca.key -cert ca.crt -revoke $certs_dir/client.goofus.crt"
# Cert still works without OCSP activation
system "$argv sql --certs-dir=$certs_dir --user=goofus -e 'select 1'"

# Enable OCSP without a running OCSP server
system "$argv sql --certs-dir=$certs_dir -e \"set cluster setting security.ocsp.mode='lax'\""
# Still works in lax mode,
system "$argv sql --certs-dir=$certs_dir --user=goofus -e 'select 1'"
system "$argv sql --certs-dir=$certs_dir --user=gallant -e 'select 1'"
system "$argv sql --certs-dir=$certs_dir -e \"set cluster setting security.ocsp.mode='strict'\""
# but fails in strict mode.
spawn $argv "sql" "--certs-dir=$certs_dir" "--user=goofus" "-e" "select 1"
# Unfortunately this error message comes from the go stdlib and we can't provide more information
eexpect "bad certificate"
expect_exit_status 1
# In strict mode even a valid cert is rejected while the server is down.
spawn $argv "sql" "--certs-dir=$certs_dir" "--user=gallant" "-e" "select 1"
eexpect "bad certificate"
expect_exit_status 1

# Now start the OCSP server. Good cert works, revoked cert doesn't.
report "STARTING OCSP SERVER"
set ocsp_pid [spawn "openssl" "ocsp" "-port" "1234" "-CA" "ca.crt" "-index" "index.txt" "-rsigner" "ca.crt" "-rkey" "ca.key"]
eexpect "waiting for OCSP client connections"
start_test "GOOD CERT IN STRICT MODE"
system "$argv sql --certs-dir=$certs_dir --user=gallant -e 'select 1'"
end_test
start_test "BAD CERT IN STRICT MODE"
spawn $argv "sql" "--certs-dir=$certs_dir" "--user=goofus" "-e" "select 1"
eexpect "bad certificate"
expect_exit_status 1
end_test


# That was in strict mode. Try again in lax mode.
system "$argv sql --certs-dir=$certs_dir -e \"set cluster setting security.ocsp.mode='lax'\""
start_test "GOOD CERT IN LAX MODE"
system "$argv sql --certs-dir=$certs_dir --user=gallant -e 'select 1'"
end_test
start_test "BAD CERT IN LAX MODE"
spawn $argv "sql" "--certs-dir=$certs_dir" "--user=goofus" "-e" "select 1"
eexpect "bad certificate"
expect_exit_status 1
end_test

# Suspend the OCSP process to test timeouts (still in lax mode, so both pass)
system "kill -STOP $ocsp_pid"
start_test "GOOD CERT IN LAX MODE WITH STOPPED SERVER"
system "$argv sql --certs-dir=$certs_dir --user=gallant -e 'select 1'"
end_test
start_test "BAD CERT IN LAX MODE WITH STOPPED SERVER"
system "$argv sql --certs-dir=$certs_dir --user=goofus -e 'select 1'"
end_test

# One more time, strict mode with stopped server (so both fail)
system "$argv sql --certs-dir=$certs_dir -e \"set cluster setting security.ocsp.mode='strict'\""
start_test "GOOD CERT IN STRICT MODE WITH STOPPED SERVER"
spawn $argv "sql" "--certs-dir=$certs_dir" "--user=gallant" "-e" "select 1"
eexpect "bad certificate"
expect_exit_status 1
end_test
start_test "BAD CERT IN STRICT MODE WITH STOPPED SERVER"
spawn $argv "sql" "--certs-dir=$certs_dir" "--user=goofus" "-e" "select 1"
eexpect "bad certificate"
expect_exit_status 1
end_test

report "CLEANING UP OCSP SERVER"
system "kill -CONT $ocsp_pid"
system "kill $ocsp_pid"
wait

stop_secure_server $argv $certs_dir

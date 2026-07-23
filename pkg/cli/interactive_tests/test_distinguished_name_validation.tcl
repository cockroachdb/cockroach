#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]
variable certs_dir "my-safe-directory"
variable custom_ca_dir "custom-ca-directory"
variable db_dir "logs/db"

set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"
spawn /bin/bash
send "PS1=':''/# '\r"

variable prompt ":/# "
eexpect $prompt


send "mkdir -p $certs_dir\r"
eexpect $prompt
send "mkdir -p $custom_ca_dir\r"
eexpect $prompt

send "$argv cert create-ca --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt

# Copy openssl CA cnf for generating custom client certs.
set ca_cnf_file [file join [file dirname $argv0] "ocsp_ca.cnf"]
send "cp $ca_cnf_file $custom_ca_dir/ca.cnf\r"
eexpect $prompt

report "GENERATING serial.txt index.txt files"
send "touch index.txt; echo '01' > serial.txt\r"
eexpect $prompt

send "$argv cert create-node localhost --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt
send "$argv cert create-client root --certs-dir=$certs_dir --ca-key=$certs_dir/ca.key\r"
eexpect $prompt

proc start_secure_server {argv certs_dir extra} {
    report "BEGIN START SECURE SERVER"
    send "$argv start-single-node --host=localhost --socket-dir=. --certs-dir=$certs_dir --store=$::db_dir --pid-file=server_pid --background $extra >>expect-cmd.log 2>&1\r";
    eexpect $::prompt
    send "$argv sql --certs-dir=$certs_dir -e 'select 1'\r";
    eexpect $::prompt
    report "SETTING COCKROACH DEV LICENSE KEY"
    set stmt1 "SET CLUSTER SETTING enterprise.license = \"$::env(COCKROACH_DEV_LICENSE)\";"
    set stmt2 "SET CLUSTER SETTING cluster.organization = \"Cockroach Labs - Production Testing\";"
    set license_sql_file "license_settings.sql"
    set fo [open $license_sql_file w]
    foreach sql_stmts {stmt1 stmt2} {
        puts $fo [set $sql_stmts]
    }
    close $fo
    system "$argv sql --certs-dir=$certs_dir --file $license_sql_file"
    report "END START SECURE SERVER"
}

proc restart_secure_server_distinguished_name_flags {argv certs_dir root_dn node_dn extra} {
    stop_server $argv
    report "BEGIN START SECURE SERVER WITH DN FLAGS"
    send "rm -f server_pid;
            $argv start-single-node --host=localhost --root-cert-distinguished-name='$root_dn' --node-cert-distinguished-name='$node_dn' --socket-dir=. --certs-dir=$certs_dir --store=$::db_dir --pid-file=server_pid --background $extra >>expect-cmd.log 2>&1;\r"
    eexpect $::prompt
    report "END START SECURE SERVER WITH DN FLAGS"
}

proc expect_exit_status {expected} {
    set status [lindex [wait] 3]
    if {$status != $expected} {
        report "unexpected exit status $status"
        exit 1
    }
}

proc create_user_cert {argv certs_dir name} {
    report "GENERATING CERT FOR USER $name"
    send "openssl genrsa -out $::certs_dir/client.$name.key\r"
    eexpect $::prompt
    send "openssl req -new -key $::certs_dir/client.$name.key -out client.$name.csr -batch -subj /O=Cockroach/CN=$name\r"
    eexpect $::prompt
    send "openssl ca -config $::custom_ca_dir/ca.cnf -keyfile $::certs_dir/ca.key -cert $::certs_dir/ca.crt -policy signing_policy -extensions signing_client_req -out $certs_dir/client.$name.crt -outdir . -in client.$name.csr -batch\r"
    eexpect $::prompt
    # Uncomment the next line to see more details about the generated cert
    #system "openssl x509 -in $::certs_dir/client.$name.crt -text"
    send "$argv sql --certs-dir=$::certs_dir -e 'create user $name'\r"
    eexpect $::prompt
    send "$argv sql --certs-dir=$::certs_dir --user=$name -e 'select 1'\r"
    eexpect $::prompt
}

proc generate_root_or_node_cert {argv certs_dir name} {
    report "GENERATING CERT FOR USER $name"
    send "openssl genrsa -out $::certs_dir/client.$name.key \r"
    eexpect $::prompt
    send "openssl req -new -key $::certs_dir/client.$name.key -out client.$name.csr -batch -subj /O=Cockroach/CN=$name \r"
    eexpect $::prompt
    send "openssl ca -config $::custom_ca_dir/ca.cnf -keyfile $::certs_dir/ca.key -cert $::certs_dir/ca.crt -policy signing_policy -extensions signing_client_req -out $certs_dir/client.$name.crt -outdir . -in client.$name.csr -batch \r"
    eexpect $::prompt
    # Uncomment the next line to see more details about the generated cert
    #system "openssl x509 -in $::certs_dir/client.$name.crt -text"
    send "$argv sql --certs-dir=$::certs_dir --user=$name -e 'select 1' \r"
    eexpect $::prompt
}

proc set_role_subject_for_user {argv name role_subject} {
    report "SETTING SUBJECT ROLE OPTION for USER $name with SUBJECT $role_subject"
    send "$argv sql --certs-dir=$::certs_dir -e 'alter role $name with subject \"$role_subject\" login';\r"
    eexpect "ALTER ROLE"
}

start_secure_server $argv $certs_dir ""

# Create users and make sure they can each log in.
create_user_cert $argv $certs_dir goofus
create_user_cert $argv $certs_dir gallant

# Check cert still works without setting role subject option
send "$argv sql --certs-dir=$certs_dir --user=goofus -e 'select 1'\r"
eexpect $::prompt

report "Validating subject role option can be set and enforced"

# Set invalid role subject option for user and check login fails
set_role_subject_for_user $argv goofus "O=NotCockroach,CN=invalid"
start_test "invalid role option for cert user goofus"
send "$argv sql --certs-dir=$certs_dir --user=goofus -e 'select 1'\r"
eexpect "certificate authentication failed for user \"goofus\""
end_test

# Set valid role subject option for user and check login passes
set_role_subject_for_user $argv goofus "O=Cockroach,CN=goofus"
start_test "valid role option for cert user goofus"
send "$argv sql --certs-dir=$certs_dir --user=goofus -e 'select 1'\r"
eexpect $::prompt
end_test

report "Validating node-cert-distinguished-nane and root-cert-distinguished-name can be set and enforced"
# create a new root certificate using custom ca
send "rm -f $certs_dir/client.root.*\r"
eexpect $::prompt
generate_root_or_node_cert $argv $certs_dir root

# Set invalid root-cert-distinguished-nane for cockroach server and check root login fails
set root_dn "O=foo,CN=invalid"
# need to provide correct node dn to start cockroach server as it depends on this to be equal to node.crt dn subject
set node_dn "O=Cockroach,CN=node"
restart_secure_server_distinguished_name_flags $argv $certs_dir "O=foo,CN=invalid" "O=bar,CN=invalid" ""
start_test "invalid root-cert-distinguished-name"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "certificate authentication failed for user \"root\""
end_test

# Set valid root-cert-distinguished-nane for cockroach server and check login passes
stop_server $argv
restart_secure_server_distinguished_name_flags $argv $certs_dir "O=Cockroach,CN=root" "O=Cockroach,CN=node" ""
start_test "valid node-cert-distinguished-nane and root-cert-distinguished-name"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect $::prompt
end_test

# Check cert still works without setting role subject option when cluster setting subject_required = false
send "$argv sql --certs-dir=$certs_dir --user=gallant -e 'select 1'\r"
eexpect $::prompt

# Check cert fails without setting role subject option when cluster setting subject_required = true
start_test "validating cert auth fails without subject role option when subject_required cluster setting is set"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'SET CLUSTER SETTING security.client_cert.subject_required.enabled = true'\r"
eexpect $::prompt
send "$argv sql --certs-dir=$certs_dir --user=gallant -e 'select 1'\r"
eexpect "user \"gallant\" does not have a distinguished name set which subject_required cluster setting mandates"
end_test

# Check cert succeeds after setting role subject option when cluster setting subject_required = true
start_test "validating cert auth succeeds with valid subject role option when subject_required cluster setting is set"
set_role_subject_for_user $argv gallant "O=Cockroach,CN=gallant"
send "$argv sql --certs-dir=$certs_dir --user=gallant -e 'select 1'\r"
eexpect "(1 row)"
end_test

# Check that cert auth fails when role subject and HBAconf(name-remapping) setting are both set for same db user."
start_test "validating cert auth fails when both role subject and HBAconf(name-remapping) setting are set"
send "rm -f $certs_dir/client.gallant.*\r"
eexpect $prompt

set id_map_stmt "SET CLUSTER SETTING server.identity_map.configuration='crdb goofus gallant'"
set hba_conf_stmt "SET CLUSTER SETTING server.host_based_authentication.configuration='hostssl all gallant all cert map=crdb'"
send "$argv sql --certs-dir=$certs_dir --user=root -e \"$id_map_stmt\" \r"
send "$argv sql --certs-dir=$certs_dir --user=root -e \"$hba_conf_stmt\" \r"
set auth_url "postgresql://gallant@localhost:26257?sslcert=$certs_dir/client.goofus.crt&sslkey=$certs_dir/client.goofus.key"
send "$argv sql --certs-dir=$certs_dir --url=\"$auth_url\" -e 'select 1'\r";
eexpect "ERROR: certificate authentication failed for user \"goofus\" (DN: o=Cockroach,cn=gallant)"
end_test

stop_server $argv

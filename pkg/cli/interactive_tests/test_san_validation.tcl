#! /usr/bin/env expect -f
#
# Tests for SAN (Subject Alternative Name) based certificate authentication.
# Validates:
#   - SAN flags require san_required cluster setting to enforce
#   - Root SAN validation (positive, negative, cross-user)
#   - Multiple SAN types (DNS, URI, IP)
#   - SAN + DN OR logic (fallback from SAN to DN)
#   - SAN identity mapping via HBA

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

# create_cert_with_dns_san generates a client certificate signed by the test CA
# with the specified organization and DNS SANs. dns_sans is a comma-separated
# list of DNS names (e.g. "foo.example.com,bar.example.com") or empty string
# for no SANs. Uses send/eexpect (like test_distinguished_name_validation.tcl)
# to ensure commands run reliably in Docker CI.
proc create_cert_with_dns_san {name org dns_sans} {
    report "GENERATING CERT FOR $name WITH DNS SANs: $dns_sans, O=$org"
    send "openssl genrsa -out $::certs_dir/client.$name.key 2048\r"
    eexpect $::prompt
    if {$dns_sans ne ""} {
        set san_parts {}
        foreach dns [split $dns_sans ","] {
            lappend san_parts "DNS:$dns"
        }
        set san_str [join $san_parts ","]
        send "openssl req -new -key $::certs_dir/client.$name.key -out client.$name.csr -batch -subj '/O=$org/CN=$name' -addext 'subjectAltName=$san_str'\r"
    } else {
        send "openssl req -new -key $::certs_dir/client.$name.key -out client.$name.csr -batch -subj '/O=$org/CN=$name'\r"
    }
    eexpect $::prompt
    send "openssl ca -config $::custom_ca_dir/ca.cnf -keyfile $::certs_dir/ca.key -cert $::certs_dir/ca.crt -policy signing_policy -extensions signing_client_req -out $::certs_dir/client.$name.crt -outdir . -in client.$name.csr -batch\r"
    eexpect $::prompt
    send "test -f $::certs_dir/client.$name.crt || echo 'CERT GENERATION FAILED'\r"
    eexpect $::prompt
}

# create_cert_with_san_ext generates a client certificate with arbitrary SAN
# types. san_ext is the full SAN extension string in openssl format
# (e.g. "DNS:foo.example.com,URI:spiffe://example.org/root,IP:127.0.0.1").
proc create_cert_with_san_ext {name org san_ext} {
    report "GENERATING CERT FOR $name WITH SANs: $san_ext, O=$org"
    send "openssl genrsa -out $::certs_dir/client.$name.key 2048\r"
    eexpect $::prompt
    send "openssl req -new -key $::certs_dir/client.$name.key -out client.$name.csr -batch -subj '/O=$org/CN=$name' -addext 'subjectAltName=$san_ext'\r"
    eexpect $::prompt
    send "openssl ca -config $::custom_ca_dir/ca.cnf -keyfile $::certs_dir/ca.key -cert $::certs_dir/ca.crt -policy signing_policy -extensions signing_client_req -out $::certs_dir/client.$name.crt -outdir . -in client.$name.csr -batch\r"
    eexpect $::prompt
    send "test -f $::certs_dir/client.$name.crt || echo 'CERT GENERATION FAILED'\r"
    eexpect $::prompt
}

proc create_cert_no_san {name org} {
    create_cert_with_dns_san $name $org ""
}

proc replace_root_cert_dns_san {dns_sans} {
    send "rm -f $::certs_dir/client.root.crt $::certs_dir/client.root.key\r"
    eexpect $::prompt
    create_cert_with_dns_san root Cockroach $dns_sans
}

proc replace_root_cert_san_ext {san_ext} {
    send "rm -f $::certs_dir/client.root.crt $::certs_dir/client.root.key\r"
    eexpect $::prompt
    create_cert_with_san_ext root Cockroach $san_ext
}

proc replace_root_cert_no_san {} {
    send "rm -f $::certs_dir/client.root.crt $::certs_dir/client.root.key\r"
    eexpect $::prompt
    create_cert_no_san root Cockroach
}

proc replace_root_cert_dns_san_with_org {org dns_sans} {
    send "rm -f $::certs_dir/client.root.crt $::certs_dir/client.root.key\r"
    eexpect $::prompt
    create_cert_with_dns_san root $org $dns_sans
}

proc start_secure_server {argv certs_dir extra} {
    report "BEGIN START SECURE SERVER"
    system "rm -f server_pid"
    send "$argv start-single-node --host=localhost --socket-dir=. --certs-dir=$certs_dir --store=$::db_dir --pid-file=server_pid --background $extra >>expect-cmd.log 2>&1\r"
    eexpect $::prompt
    send "$argv sql --certs-dir=$certs_dir -e 'select 1'\r"
    eexpect $::prompt
    report "END START SECURE SERVER"
}

proc restart_secure_server_san_flags {argv certs_dir root_san node_san extra} {
    stop_server $argv
    report "BEGIN RESTART SECURE SERVER WITH SAN FLAGS"
    system "rm -f server_pid"
    send "$argv start-single-node --host=localhost --root-cert-san='$root_san' --node-cert-san='$node_san' --socket-dir=. --certs-dir=$certs_dir --store=$::db_dir --pid-file=server_pid --background $extra >>expect-cmd.log 2>&1\r"
    eexpect $::prompt
    # Wait for server to be ready before proceeding.
    send "$argv sql --certs-dir=$certs_dir -e 'select 1'\r"
    eexpect $::prompt
    report "END RESTART SECURE SERVER WITH SAN FLAGS"
}

proc restart_secure_server_san_and_dn_flags {argv certs_dir root_san node_san root_dn node_dn extra} {
    stop_server $argv
    report "BEGIN RESTART SECURE SERVER WITH SAN AND DN FLAGS"
    system "rm -f server_pid"
    send "$argv start-single-node --host=localhost --root-cert-san='$root_san' --node-cert-san='$node_san' --root-cert-distinguished-name='$root_dn' --node-cert-distinguished-name='$node_dn' --socket-dir=. --certs-dir=$certs_dir --store=$::db_dir --pid-file=server_pid --background $extra >>expect-cmd.log 2>&1\r"
    eexpect $::prompt
    # Wait for server to be ready before proceeding.
    send "$argv sql --certs-dir=$certs_dir -e 'select 1'\r"
    eexpect $::prompt
    report "END RESTART SECURE SERVER WITH SAN AND DN FLAGS"
}

# Core DNS SAN validation tests.
# Server is started with --root-cert-san and --node-cert-san flags.
# Tests verify that these flags only take effect when the san_required cluster
# setting is enabled, and that SAN matching works for root and regular users
# under various conditions.

replace_root_cert_dns_san "wrong.example.com"

start_secure_server $argv $certs_dir "--root-cert-san='DNS=root.example.com' --node-cert-san='DNS=node.example.com'"

send "$argv sql --certs-dir=$certs_dir -e 'CREATE USER testuser'\r"
eexpect $prompt

# Verify that SAN startup flags alone do not enforce validation when the
# san_required cluster setting is false. The root cert has a wrong SAN but
# authentication should still pass via CN matching.
start_test "san_required=false with SAN flags does not enforce SAN validation"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test

# Restore valid root cert and enable the san_required cluster setting for
# subsequent tests.
replace_root_cert_dns_san "root.example.com"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'SET CLUSTER SETTING security.client_cert.san_required.enabled = true'\r"
eexpect "SET CLUSTER SETTING"
eexpect $prompt

# Verify that root can authenticate when the cert contains the expected DNS SAN.
start_test "root with matching DNS SAN authenticates successfully"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test

# Verify that a regular user (non-root/node) without HBA identity mapping can
# still authenticate when san_required is true. SAN validation is only enforced
# for root and node users; regular users bypass SAN checks.
start_test "regular user authenticates when san_required=true without HBA map"
create_cert_no_san testuser Cockroach
send "$argv sql --certs-dir=$certs_dir --user=testuser -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test

# Verify that root authentication fails when the cert contains a DNS SAN that
# does not match the configured --root-cert-san value.
start_test "root with wrong DNS SAN is rejected"
replace_root_cert_dns_san "wrong.example.com"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "certificate authentication failed for user \"root\" based on SAN entries"
eexpect $prompt
end_test
replace_root_cert_dns_san "root.example.com"

# Verify that root authentication fails when the cert has no SANs at all.
start_test "root with no SAN is rejected when san_required=true"
replace_root_cert_no_san
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "certificate authentication failed for user \"root\" based on SAN entries"
eexpect $prompt
end_test
replace_root_cert_dns_san "root.example.com"

# Verify that root cannot authenticate using a cert that contains the node's
# SAN rather than root's SAN. Each user is only checked against their own
# configured SAN.
start_test "root with node SAN is rejected"
replace_root_cert_dns_san "node.example.com"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "certificate authentication failed for user \"root\" based on SAN entries"
eexpect $prompt
end_test
replace_root_cert_dns_san "root.example.com"

# Verify that a cert with extra SANs beyond the configured one still passes.
# The check is that all configured SANs are a subset of cert SANs.
start_test "root with superset of configured SANs authenticates successfully"
replace_root_cert_dns_san "root.example.com,extra.example.com"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test
replace_root_cert_dns_san "root.example.com"

# Multiple SAN type validation tests.
# Restart server with root configured to require DNS, URI, and IP SANs.
# Validates that all configured SAN types must be present in the cert.

replace_root_cert_san_ext "DNS:root.example.com,URI:spiffe://example.org/root,IP:127.0.0.1"

restart_secure_server_san_flags $argv $certs_dir \
    "DNS=root.example.com,URI=spiffe://example.org/root,IP=127.0.0.1" \
    "DNS=node.example.com" ""

# Verify that a cert containing all three configured SAN types passes.
start_test "root with DNS, URI, and IP SANs authenticates successfully"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test

# Verify that a missing URI SAN causes authentication failure even when DNS
# and IP SANs are present.
start_test "root missing URI SAN is rejected"
replace_root_cert_san_ext "DNS:root.example.com,IP:127.0.0.1"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "certificate authentication failed for user \"root\" based on SAN entries"
eexpect $prompt
end_test
replace_root_cert_san_ext "DNS:root.example.com,URI:spiffe://example.org/root,IP:127.0.0.1"

# Verify that a missing IP SAN causes authentication failure even when DNS
# and URI SANs are present.
start_test "root missing IP SAN is rejected"
replace_root_cert_san_ext "DNS:root.example.com,URI:spiffe://example.org/root"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "certificate authentication failed for user \"root\" based on SAN entries"
eexpect $prompt
end_test
replace_root_cert_san_ext "DNS:root.example.com,URI:spiffe://example.org/root,IP:127.0.0.1"

# SAN and DN OR-logic tests.
# Restart server with both SAN and DN flags configured. When both are set,
# authentication succeeds if either the SAN or DN matches. This tests the
# fallback from SAN to DN validation.

replace_root_cert_dns_san "root.example.com"

restart_secure_server_san_and_dn_flags $argv $certs_dir \
    "DNS=root.example.com" "DNS=node.example.com" \
    "O=Cockroach,CN=root" "O=Cockroach,CN=node" ""

send "$argv sql --certs-dir=$certs_dir --user=root -e 'SET CLUSTER SETTING security.client_cert.subject_required.enabled = true'\r"
eexpect "SET CLUSTER SETTING"
eexpect $prompt

# Verify that a matching SAN is sufficient even when the DN is wrong. SAN
# validation succeeds first and short-circuits the DN check.
start_test "valid SAN with wrong DN authenticates via SAN"
replace_root_cert_dns_san_with_org "WrongOrg" "root.example.com"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test

# Verify that a matching DN is sufficient when the SAN is wrong. After SAN
# validation fails, authentication falls through to DN matching.
start_test "wrong SAN with valid DN authenticates via DN fallback"
replace_root_cert_dns_san "wrong.example.com"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test

# Verify that authentication fails when both SAN and DN are wrong. The error
# message should mention DN since that is the last validation path exercised.
start_test "wrong SAN and wrong DN both fail"
replace_root_cert_dns_san_with_org "WrongOrg" "wrong.example.com"
send "$argv sql --certs-dir=$certs_dir --user=root -e 'select 1'\r"
eexpect "certificate authentication failed for user \"root\" (DN:"
eexpect $prompt
end_test
replace_root_cert_dns_san "root.example.com"

# SAN-based identity mapping tests.
# Restart server with SAN flags. Configure HBA and identity map so that a
# cert's SAN entry maps to a database user via the identity map, instead of
# relying on CN matching.

restart_secure_server_san_flags $argv $certs_dir \
    "DNS=root.example.com" "DNS=node.example.com" ""

send "$argv sql --certs-dir=$certs_dir -e 'CREATE USER IF NOT EXISTS gallant'\r"
eexpect $prompt
send "$argv sql --certs-dir=$certs_dir -e 'CREATE USER IF NOT EXISTS impostor'\r"
eexpect $prompt
send "$argv sql --certs-dir=$certs_dir -e 'GRANT ALL ON DATABASE defaultdb TO gallant'\r"
eexpect $prompt
send "$argv sql --certs-dir=$certs_dir -e 'GRANT ALL ON DATABASE defaultdb TO impostor'\r"
eexpect $prompt

create_cert_with_dns_san goofus Cockroach "goofus.example.com"

send "$argv sql --certs-dir=$certs_dir --user=root -e \"SET CLUSTER SETTING server.identity_map.configuration='crdb SAN:DNS:goofus.example.com gallant'\"\r"
eexpect "SET CLUSTER SETTING"
eexpect $prompt
send "$argv sql --certs-dir=$certs_dir --user=root -e \"SET CLUSTER SETTING server.host_based_authentication.configuration='hostssl all gallant all cert map=crdb\nhostssl all impostor all cert map=crdb\nhostssl all all all cert'\"\r"
eexpect "SET CLUSTER SETTING"
eexpect $prompt

# Verify that a cert's DNS SAN can be mapped to a database user via the
# identity map. The goofus cert's SAN maps to gallant through the identity
# map, allowing authentication as gallant.
start_test "SAN identity mapped to database user via HBA authenticates successfully"
set auth_url "postgresql://gallant@localhost:26257?sslcert=$certs_dir/client.goofus.crt&sslkey=$certs_dir/client.goofus.key&sslrootcert=$certs_dir/ca.crt&sslmode=verify-full"
send "$argv sql --url=\"$auth_url\" -e 'select 1'\r"
eexpect "(1 row)"
eexpect $prompt
end_test

# Verify that authentication fails when san_required is true and the cert has
# no SANs, even when HBA mapping is configured. This is the only code path
# that enforces SAN presence for non-root/node users.
start_test "cert without SANs is rejected when san_required=true with HBA map"
send "rm -f $certs_dir/client.goofus.crt $certs_dir/client.goofus.key\r"
eexpect $prompt
create_cert_no_san goofus Cockroach
set auth_url "postgresql://gallant@localhost:26257?sslcert=$certs_dir/client.goofus.crt&sslkey=$certs_dir/client.goofus.key&sslrootcert=$certs_dir/ca.crt&sslmode=verify-full"
send "$argv sql --url=\"$auth_url\" -e 'select 1'\r"
eexpect "client certificate SAN is required"
eexpect $prompt
end_test

# Verify that authentication fails when san_required is true, HBA identity
# mapping is configured, and the cert has a SAN that does not match the
# identity map entry. The goofus cert has "wrong.example.com" but the map
# expects "goofus.example.com".
start_test "wrong SAN with HBA map is rejected"
send "rm -f $certs_dir/client.goofus.crt $certs_dir/client.goofus.key\r"
eexpect $prompt
create_cert_with_dns_san goofus Cockroach "wrong.example.com"
set auth_url "postgresql://gallant@localhost:26257?sslcert=$certs_dir/client.goofus.crt&sslkey=$certs_dir/client.goofus.key&sslrootcert=$certs_dir/ca.crt&sslmode=verify-full"
send "$argv sql --url=\"$auth_url\" -e 'select 1'\r"
eexpect "did not map to any database role"
eexpect $prompt
end_test

# Verify that a cert's SAN identity mapping is enforced: connecting as a user
# that the SAN does not map to is rejected. The goofus cert's SAN maps to
# gallant, so connecting as impostor must fail.
start_test "SAN mapped to different user than requested is rejected"
send "rm -f $certs_dir/client.goofus.crt $certs_dir/client.goofus.key\r"
eexpect $prompt
create_cert_with_dns_san goofus Cockroach "goofus.example.com"
set auth_url "postgresql://impostor@localhost:26257?sslcert=$certs_dir/client.goofus.crt&sslkey=$certs_dir/client.goofus.key&sslrootcert=$certs_dir/ca.crt&sslmode=verify-full"
send "$argv sql --url=\"$auth_url\" -e 'select 1'\r"
eexpect "does not correspond to any mapping"
eexpect $prompt
end_test

stop_server $argv

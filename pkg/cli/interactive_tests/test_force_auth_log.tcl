#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"
set certs_dir "/certs"

set ::env(COCKROACH_ALWAYS_LOG_SERVER_IDS) 1
set ::env(COCKROACH_ALWAYS_LOG_AUTHN_EVENTS) 1

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


start_secure_server $argv $certs_dir ""

set logfile logs/db/logs/cockroach-auth.log

# run a client command, so we have at least one authn event in the log.
system "$argv sql -e 'create user someuser' --certs-dir=$certs_dir"
system "$argv sql -e 'select 1' --user someuser --certs-dir=$certs_dir</dev/null || true"

start_test "Check that the authentication events are reported"

system "grep -q 'authentication succeeded' $logfile"
system "grep -q 'authentication failed' $logfile"

end_test

start_test "Check that the auth events have both node ID and cluster ID"

system "grep -q '\\\[n1,.*clusterID=........-....-....-....-............\\\] . authentication' $logfile"

end_test

stop_secure_server $argv $certs_dir

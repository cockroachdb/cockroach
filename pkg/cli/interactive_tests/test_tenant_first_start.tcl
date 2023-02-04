#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

system "$argv sql --no-line-editor -e 'CREATE TENANT foo; ALTER TENANT foo START SERVICE SHARED'"

# Nuke the tenant keyspace from underneath the tenant record. this
# ensures that the service will always fail to start.
system "echo '{\"requests\": \[{\"deleteRange\": {\"header\": {\"key\": \"/oo=\", \"endKey\": \"//8=\"}}}\]}' | $argv debug send-kv-batch"

stop_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

start_test "Check that the server can start properly if prestart is disabled."
# Given that the tenant is busted, without the flag the server would fail to start
# properly.
send "$argv start-single-node --insecure --store=logs/db --disable-prestart-tenant-servers\r"
eexpect "CockroachDB node starting"
eexpect "restarted pre-existing node"
interrupt
eexpect "interrupted"
eexpect $prompt
end_test

send "exit\r"
eexpect eof

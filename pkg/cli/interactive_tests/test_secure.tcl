#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ca_crt "/certs/ca.crt"
set node_crt "/certs/node.crt"
set node_key "/certs/node.key"
set root_crt "/certs/root.crt"
set root_key "/certs/root.key"

proc start_secure_server {argv ca_crt node_crt node_key} {
    system "$argv start --ca-cert=$ca_crt --cert=$node_crt --key=$node_key & echo \$! > server_pid"
    sleep 1
}

start_secure_server $argv $ca_crt $node_crt $node_key

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

send "$argv node ls --ca-cert=$ca_crt --cert=$node_crt --key=$node_key\r"
eexpect "id"
eexpect "1"
eexpect "1 row"

eexpect $prompt

# Invalid combination.
send "$argv sql --ca-cert=$ca_crt --cert=$node_crt\r"
eexpect "Error: missing --key flag"
eexpect "Failed running \"sql\""

eexpect $prompt

# CA cert must be specified regardless of authentication mode.
send "$argv sql\r"
eexpect "cleartext connections are not permitted"

eexpect $prompt

# A nonexistent user cannot authenticate with either form of authentication.
send "$argv sql --user=nonexistent --ca-cert=$ca_crt --cert=$node_crt --key=$node_key\r"
eexpect "user nonexistent does not exist"

eexpect $prompt

# Root can only authenticate using certificate authentication.
send "$argv sql --ca-cert=$ca_crt\r"
eexpect "user root must authenticate using a client certificate"

eexpect $prompt

send "$argv user set carl --password --ca-cert=$ca_crt --cert=$root_crt --key=$root_key\r"
eexpect "Enter password:"
send "woof\r"
eexpect "Confirm password:"
send "woof\r"
eexpect "INSERT 1\r\n"

eexpect $prompt

send "$argv sql --ca-cert=$ca_crt --user=carl\r"
eexpect "Enter password:"
send "woof\r"
eexpect "Confirm password:"
send "woof\r"
eexpect "carl@"

# Terminate with Ctrl+C.
send "\003"

eexpect $prompt

send "exit 0\r"
eexpect eof

stop_server $argv

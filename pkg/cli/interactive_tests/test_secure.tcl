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

spawn $argv node ls --ca-cert=$ca_crt --cert=$node_crt --key=$node_key
eexpect "id"
eexpect "1"
eexpect "1 row"

# Invalid combination.
spawn $argv sql --ca-cert=$ca_crt --cert=$node_crt
eexpect "Error: missing --key flag"
eexpect "Failed running \"sql\""

# CA cert must be specified regardless of authentication mode.
spawn $argv sql
eexpect "cleartext connections are not permitted"

# A nonexistent user cannot authenticate with either form of authentication.
spawn $argv sql --user=nonexistent --ca-cert=$ca_crt --cert=$node_crt --key=$node_key
eexpect "user nonexistent does not exist"

# Root can only authenticate using certificate authentication.
spawn $argv sql --ca-cert=$ca_crt
eexpect "user root cannot authenticate using a password"

spawn $argv sql --ca-cert=$ca_crt --cert=$root_crt --key=$root_key
eexpect "root@"
send "CREATE USER carl WITH PASSWORD 'woof';\r"
eexpect "CREATE USER"

spawn $argv sql --ca-cert=$ca_crt --user=carl
eexpect "Enter password:"
send "woof\r"
eexpect "Confirm password:"
send "woof\r"
eexpect "carl@"

stop_server $argv

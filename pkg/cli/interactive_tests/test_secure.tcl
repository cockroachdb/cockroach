#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

proc start_secure_server {argv} {
    system "$argv start --ca-cert=/certs/ca.crt --cert=/certs/node.crt --key=/certs/node.key & echo \$! > server_pid"
    sleep 1
}

start_secure_server $argv

spawn $argv node ls --ca-cert=/certs/ca.crt --cert=/certs/node.crt --key=/certs/node.key
eexpect "id"
eexpect "1"
eexpect "1 row"

stop_server $argv

#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"

set ca_crt "/certs/ca.crt"
set node_crt "/certs/node.crt"
set node_key "/certs/node.key"

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

send "$argv start --certs-dir= --ca-cert=$ca_crt --cert=$node_crt --key=$node_key\r"
eexpect "ERROR: failed to start Cockroach server: problem using security settings"

eexpect $prompt

send "exit 0\r"
eexpect eof

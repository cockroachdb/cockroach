#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"

set ca_crt "/certs/ca.crt"
set node_crt "/certs/node.crt"
set node_key "/certs/node.key"

proc start_secure_server {argv ca_crt node_crt node_key} {
    system "mkfifo pid_fifo || true; $argv start --certs-dir= --ca-cert=$ca_crt --cert=$node_crt --key=$node_key --pid-file=pid_fifo & cat pid_fifo > server_pid"
}

proc stop_secure_server {argv certs_dir} {
    system "set -e; if kill -CONT `cat server_pid`; then $argv quit --certs-dir=$certs_dir || true & sleep 1; kill -9 `cat server_pid` || true; else $argv quit --certs-dir=$certs_dir || true; fi"
}

start_secure_server $argv $ca_crt $node_crt $node_key

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

send "$argv sql --certs-dir=$certs_dir\r"
eexpect "root@"

# Terminate with Ctrl+C.
interrupt

eexpect $prompt

send "exit 0\r"
eexpect eof

stop_secure_server $argv $certs_dir

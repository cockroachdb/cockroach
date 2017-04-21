#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"

proc start_secure_server {argv certs_dir} {
    system "mkfifo pid_fifo || true; $argv start --certs-dir=$certs_dir --pid-file=pid_fifo >>stdout.log 2>>stderr.log & cat pid_fifo > server_pid"
}

proc stop_secure_server {argv certs_dir} {
    system "set -e; if kill -CONT `cat server_pid`; then $argv quit --certs-dir=$certs_dir || true & sleep 1; kill -9 `cat server_pid` || true; else $argv quit --certs-dir=$certs_dir || true; fi"
}

start_secure_server $argv $certs_dir

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

send "$argv node ls --certs-dir=$certs_dir\r"
eexpect "id"
eexpect "1"
eexpect "1 row"

eexpect $prompt

# Cannot create users with empty passwords.
send "$argv user set carl --password --certs-dir=$certs_dir\r"
eexpect "Enter password:"
send "\r"
eexpect "empty passwords are not permitted"

eexpect $prompt

send "$argv user set carl --password --certs-dir=$certs_dir\r"
eexpect "Enter password:"
send "woof\r"
eexpect "Confirm password:"
send "woof\r"
eexpect "INSERT 1\r\n"

eexpect $prompt

send "$argv sql --certs-dir=$certs_dir --user=carl\r"
eexpect "Enter password:"
send "woof\r"
eexpect "Confirm password:"
send "woof\r"
eexpect "carl@"

# Terminate with Ctrl+C.
interrupt

eexpect $prompt

send "exit 0\r"
eexpect eof

stop_secure_server $argv $certs_dir

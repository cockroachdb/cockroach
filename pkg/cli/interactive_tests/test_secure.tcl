#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"

proc start_secure_server {argv certs_dir} {
    system "mkfifo pid_fifo || true; $argv start --certs-dir=$certs_dir --pid-file=pid_fifo & cat pid_fifo > server_pid"
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

# CA cert must be specified regardless of authentication mode.
send "$argv sql\r"
eexpect "cleartext connections are not permitted"

eexpect $prompt

# A nonexistent user cannot authenticate with either form of authentication.
send "$argv sql --user=nonexistent --certs-dir=$certs_dir\r"
eexpect "user nonexistent does not exist"

eexpect $prompt

# Root can only authenticate using certificate authentication.
send "$argv sql --certs-dir=$certs_dir\r"
eexpect "user root must use certificate authentication instead of password authentication"

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

stop_server $argv

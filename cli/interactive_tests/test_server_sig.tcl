#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set timeout 15

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

# Check that the server shuts down upon receiving SIGTERM.
send "$argv start & echo $! >server_pid; fg\r"
eexpect "initialized"

system "kill `cat server_pid`"
eexpect "initiating graceful shutdown"
eexpect "shutdown completed"
eexpect ":/# "

# SIGTERM finishes with exit code 0. (#9051)
send "echo \$?\r"
eexpect "0\r\n"
eexpect ":/# "

# Check that the server shuts down upon receiving Ctrl+C.
send "$argv start & echo $! >server_pid; fg\r"
eexpect "restarted"

send "\003"
eexpect "initiating graceful shutdown"
eexpect "shutdown completed"
eexpect ":/# "

# Ctrl+C finishes with exit code 1. (#9051)
send "echo \$?\r"
eexpect "1\r\n"
eexpect ":/# "

# Check that the server shuts down fast upon receiving Ctrl+C twice.
set timeout 1
send "$argv start & echo $! >server_pid; fg\r"
eexpect "restarted"

send "\003"
eexpect "initiating graceful shutdown"
send "\003"
eexpect "hard shutdown"
eexpect ":/# "

# Ctrl+C twice finishes with exit code 130. (#9051)
send "echo \$?\r"
eexpect "130\r\n"
eexpect ":/# "

send "exit\r"
eexpect eof

stop_server $argv

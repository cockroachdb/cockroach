#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# we force TERM to xterm, otherwise we can't
# test bracketed paste below.
set env(TERM) xterm

spawn $argv demo --no-example-database

eexpect "defaultdb>"

start_test "Test that a multi-line bracketed paste is handled properly."
send "\033\[200~"
send "\\set display_format csv\r\n"
send "values (1,'a'), (2,'b'), (3,'c');\r\n"
send "\033\[201~\r\n"
eexpect "1,a"
eexpect "2,b"
eexpect "3,c"
eexpect "defaultdb>"
end_test

send_eof
eexpect eof

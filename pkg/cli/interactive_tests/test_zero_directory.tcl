#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Extract the absolute path of the crdb binary.
send "b=\$(cd \$(dirname $argv) && pwd)\r"
eexpect ":/# "
# Ditto for the current directory.
send "ppwd=\$PWD\r"
eexpect ":/# "

# Make a "zero directory": a directory without permissions.
send "rm -rf zerodir && mkdir zerodir && cd zerodir\r"
eexpect ":/# "
system "chmod 0 zerodir"
send "trap \"chmod +rwx \$ppwd/zerodir || true\" EXIT SIGHUP SIGINT\r"
eexpect ":/# "


start_test "Check that cockroach version works in a zero directory."
send "\$b/\$(basename $argv) version\r"
eexpect "Build Tag"
eexpect "Platform"
eexpect "Build Type"
eexpect ":/# "
end_test

start_test "Check that a server can be started in a zero directory."
send "\$b/\$(basename $argv) start-single-node --insecure --store=path=\$ppwd/logs/cockroach-data\r"
eexpect "CockroachDB node starting"
send "\003"
eexpect "interrupted"
eexpect ":/# "
end_test

# Clean up.
send "cd \$ppwd && chmod +rwx zerodir && rm -rf zerodir\r"
eexpect ":/# "
send "exit\r"
eexpect eof

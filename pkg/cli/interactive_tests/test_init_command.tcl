#! /usr/bin/env expect -f
#
source [file join [file dirnam $argv0] common.tcl]

# Start a server with a --join flag so the init command is required
# (even though we have no intention of starting a second node).  Note that unlike other
# expect-based tests, this one doesn't use a fifo for --pid_file
# because we don't want reads from that fifo to change the outcome.
system "$argv start --insecure --pid-file=server_pid -s=path=logs/db --listen-addr=localhost --background --join=localhost:26258 >>logs/expect-cmd.log 2>&1"

start_test "Check that the server has informed us and the log file that it was ready before forking off in the background"
system "grep -q 'initial startup completed, will now wait' logs/db/logs/cockroach.log"
end_test

start_test "Check that the SQL shell successfully times out upon connecting to an uninitialized node"
# We shorten the default timeout of 5 seconds to make this test run faster.
set ::env(COCKROACH_CONNECT_TIMEOUT) "1"
spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "
send "$argv sql\r"
eexpect "Error: cannot dial server"
send "exit\r"
eexpect eof
end_test

# The following tests expect the client to wait forever.
set ::env(COCKROACH_CONNECT_TIMEOUT) "0"


# Start a shell and send a command. The shell should block at startup,
# so we don't "expect" anything yet. The point of this test is to
# verify that the command will succeed after blocking instead of
# erroring out.
spawn $argv sql
send "show tables from system.pg_catalog;\r"

# Now initialize the one-node cluster. This will unblock the pending
# SQL connection. This also verifies that the blocked connection using
# the pgwire listener does not block the grpc listener used for the
# init command.
system "$argv init --insecure --host=localhost"

# The command should now succeed, without logging any errors or
# warnings.
expect {
    "pg_class" {}
    # Hopefully this broad regex will match any errors we log
    # (Currently, everything I've seen begins with "Error:")
    -re "(?i)err" {
        set prefix $expect_out(buffer)
        # Read next line to finish the error message.
        # TODO(bdarnell): Surely there's a smarter way to do this.
        expect "\n"
        report "ERROR LOGGED:\n$prefix$expect_out(buffer)"
        exit 1
    }
    timeout { handle_timeout "pg_class" }
}

stop_server $argv

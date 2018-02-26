#! /usr/bin/env expect -f
#
source [file join [file dirnam $argv0] common.tcl]

# Start a server with a --join flag so the init command is required
# (even though we have no intention of starting a second node). Even
# though the cluster is uninitialized, --background causes it to
# return once we have bound our ports. Note that unlike other
# expect-based tests, this one doesn't use a fifo for --pid_file
# because we don't want reads from that fifo to change the outcome.
system "$argv start --insecure --pid-file=server_pid --background -s=path=logs/db --join=localhost:26258 >>logs/expect-cmd.log 2>&1"

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
system $argv init

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

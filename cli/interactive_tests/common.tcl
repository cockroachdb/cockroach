# We are sending vt100 terminal sequences below, so inform readline
# accordingly.
set env(TERM) vt100

# Keep the history in a test location, so as to not override the
# developer's own history file.
set histfile "/crl-history"
set env(COCKROACH_SQL_CLI_HISTORY) $histfile
system "rm -f $histfile"

# Everything in this test should be fast. Don't be tolerant for long
# waits.
set timeout 1

# When run via Docker the enclosing terminal has 0 columns and 0 rows,
# and this confuses readline. Ensure sane defaults here.
set stty_init "cols 80 rows 25"

# Convenience wrapper function, which ensures that all expects are
# mandatory (i.e. with a mandatory fail if the expected output doesn't
# show up fast).
proc eexpect {text} {
    expect {
	$text {}
	timeout {exit 1}
    }
}

# Convenience functions to start/shutdown the server.
# Preserves the invariant that the server's PID is saved
# in `server_pid`.
proc start_server {argv} {
    system "$argv start &>> out & echo \$! >server_pid"
    sleep 1
}
proc stop_server {argv} {
    system "set -e; if kill -CONT `cat server_pid`; then $argv quit &>> out || true & sleep 1; kill -9 `cat server_pid` || true; else $argv quit || true; fi"
}

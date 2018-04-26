# Overview

The tests in this directory can exercise a `cockroach` binary from the
"outside", as when a user would run it from a terminal. They can be
used to test and check behavior when run interactively; when subject
to external effects from the operating system, such as pausing and
restarting the process, etc.

# How to run the tests

Two ways to run the tests are supported:

- the canonical way that mimics what CI does is to run the entire
  suite as an acceptance test with:

  `make acceptance TESTS=TestDockerCLI`

  This requires Docker. The test driver is in `acceptance/cli_test.go`.

  To enable verbose debugging output, crank up the log verbosity:

  `make acceptance TESTS=TestDockerCLI TESTFLAGS="-v -vmodule=cli_test=2"`

- for a quick check, or manual testing while authoring a test, one can
  run the test from the current shell with:

  `expect -f path/to/test.tcl path/to/cockroach`

  When used this way, care must be taken that there isn't any
  cockroach process running prior to starting the test.

  The expect flag `-d` can be added to debug the interaction.

# About Expect and Tcl

The test themselves are written using
[Expect](https://en.wikipedia.org/wiki/Expect), a library/package for
[Tcl](https://en.wikipedia.org/wiki/Tcl). Tcl is a simple programming
language with similar target use cases as a Unix shell but with richer
semantics and more regular syntax. Expect makes it possible to
automate (in Tcl) the interaction with a process when its input/output channels
are a virtual terminal, as opposed to the (more common, easier to
program) automation of redirection-based invocations.

# Tips and common pitfalls

- `send` and `expect` are unsynchronized with respect to each
  other. This is because the input and output channels to a process
  are fully separate. In particular, the strings from the process
  matched by `expect` can have been sent before or after the strings
  sent by `send` reach the process.  When testing e.g. an interactive
  shell, beware to match the command prompt with `expect` between
  interactions, for otherwise in a sequence like
  send-expect-send-expect the 2nd expect can match the remainder of
  the input produced as a result of the first send.

- `expect` works on a continuous stream of characters. The channel
  with a process is not "packetized" so there is no notion of "read
  the data that was written by the process in one go". Thus failure to
  match a string in `expect` does not necessarily imply that the
  process has "done something else entirely". It may also mean
  that the process has not output the expected string *yet* (`expect`
  has to wait some more). We use a wrapper function `eexpect` around
  `expect` that times out the interaction, but a timeout really means
  something slightly different than "incorrect output".

  *NB: this is the hardest perspective shift for newcomers to Expect.*

- Expect's `send` command does not send a carriage return unless
  directed to do so. When a test's debug output suggests the command
  is not reacting to input, check the previous `send` and whether it
  misses a carriage return.

- `send` emulates actions from a terminal. Pressing the enter key on
  the terminal can be emulated by sending a carriage return `\r`,
  whereas sending `\n` is equivalent to pressing Ctrl+J on the
  keyboard. These are not fully equivalent (a program can distinguish
  between them).  Fun fact: why do simple programs only receive `\n`
  on their standard input for both enter key presses and Ctrl+J?
  That's because the terminal does this conversion automatically unless
  instructed otherwise (by e.g. a `readline` library).

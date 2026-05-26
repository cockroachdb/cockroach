# CLI utility design guidelines

## Guidelines for operator-level utilities

- Be concise in the common case, verbose with errors.

- Be flexible on input, strict on output.
  (Accept mistakes by the user providing flags / input; but make the
  output format specific: it is consumed by programs.)

- Errors on stderr, regular output on stdout.

- CLI tools other than the `debug` utilities are part of the API:
  don't change features without release notes; use the deprecation
  cycle to remove features or change flags; and ensure cross-version
  compatibility (possibly via opt-in flags).

## Guidelines for the SQL shell

- Be mindful of the two "modes" of *output*:

  - output consumed by a human. (`cliCtx.terminalOutput == true`)

    In this mode, there can be explanations printed out to help
    the human understand what they are seeing. Long
    operations (eg workload init) should be described by an
    explanatory message before the operation starts, so that the user
    knows there is something to wait for.

  - output consumed by scripts, automation etc. (`cliCtx.terminalOutput == false`)

    In the output automation mode: output should be concise, parsable:
    it is consumed by programs.

    In the non-human case, the output is part of the public API of CockroachDB.
    Beware of documenting changes in release notes; use the deprecation cycle
    to remove key output aspects; ensure that users can preserve specific
    outputs from previous version when upgrading, possibly via opt-in flags.

- Be mindful of the two "modes" of *input*:

  - input provided by a human: interactive use via a
    terminal. (`cliCtx.isInteractive == true`)

    In this mode, there can be explanations to suggest to the human
    what is a good “next step” after they do something.

  - input provided by scripts, automation etc., e.g. via `cockroach sql <input.sql`.
    (`cliCtx.isInteractive == false`)

    In this mode, there should *not* be guidance printed out to help a
    human.

    Also, the automated input mode is part of the public API of CockroachDB.
    Beware of documenting changes in release notes; use the deprecation cycle
    to remove key input aspects; ensure that users can preserve specific
    outputs from previous version when upgrading, possibly via opt-in flags.

- Beware: the two modes above are orthogonal to each other. We know
  about real-world usage of all 4 combinations:

  - fully interactive: `terminalOutput && isInteractive`
  - fully automated: `!terminalOutput && !isInteractive`
  - human reads a SQL script from a file but wants explanations about
    what's going on, e.g. running `cockroach sql <foo.sql` on a
    terminal: `terminalOutput && !isInteractive`
  - human runs the SQL shell but filters the output, e.g. `cockroach sql | grep XXX`
    `!terminalOutput && isInteractive`.

- Be mindful of the `--embedded` flag, for use in "playground" environments.

  If `sqlCtx.embeddedMode` is set, the (human) user has no control
  over the command line and the configuration. In that case, help
  texts about the configuration should be avoided because they are
  non-actionable.

package cli

// BashCompleteFunc is an action to execute when the shell completion flag is set
type BashCompleteFunc func(*Context)

// BeforeFunc is an action to execute before any subcommands are run, but after
// the context is ready if a non-nil error is returned, no subcommands are run
type BeforeFunc func(*Context) error

// AfterFunc is an action to execute after any subcommands are run, but after the
// subcommand has finished it is run even if Action() panics
type AfterFunc func(*Context) error

// ActionFunc is the action to execute when no subcommands are specified
type ActionFunc func(*Context) error

// CommandNotFoundFunc is executed if the proper command cannot be found
type CommandNotFoundFunc func(*Context, string)

// OnUsageErrorFunc is executed if an usage error occurs. This is useful for displaying
// customized usage error messages.  This function is able to replace the
// original error messages.  If this function is not set, the "Incorrect usage"
// is displayed and the execution is interrupted.
type OnUsageErrorFunc func(context *Context, err error, isSubcommand bool) error

// ExitErrHandlerFunc is executed if provided in order to handle exitError values
// returned by Actions and Before/After functions.
type ExitErrHandlerFunc func(context *Context, err error)

// FlagStringFunc is used by the help generation to display a flag, which is
// expected to be a single line.
type FlagStringFunc func(Flag) string

// FlagNamePrefixFunc is used by the default FlagStringFunc to create prefix
// text for a flag's full name.
type FlagNamePrefixFunc func(fullName []string, placeholder string) string

// FlagEnvHintFunc is used by the default FlagStringFunc to annotate flag help
// with the environment variable details.
type FlagEnvHintFunc func(envVars []string, str string) string

// FlagFileHintFunc is used by the default FlagStringFunc to annotate flag help
// with the file path details.
type FlagFileHintFunc func(filePath, str string) string

package stream

import "os/exec"

// XargsFilter is a Filter that applies a command to every input item.
type XargsFilter struct {
	command    string
	args       []string
	limitArgs  int
	limitBytes int
}

// Xargs returns a filter that executes "command args... items..."
// where items are the input to the filter.  The handling of items may
// be split across multiple executions of command (typically to meet
// command line length restrictions).  The standard output of the
// execution(s) is split into lines and the lines form the output of
// the filter (with trailing newlines removed).
func Xargs(command string, args ...string) *XargsFilter {
	// Compute length of command+args
	base := len(command)
	for _, a := range args {
		base += 1 + len(a)
	}
	return &XargsFilter{
		command:    command,
		args:       args,
		limitArgs:  4096,
		limitBytes: 4096 - 100 - base, // Posix limit with slop
	}
}

// LimitArgs adjusts x so that no more than n input items are passed to
// a single command execution.  If not called, an unspecified number
// of input items may be handled via a single command execution.
func (x *XargsFilter) LimitArgs(n int) *XargsFilter {
	x.limitArgs = n
	return x
}

// RunFilter implements the Filter interface: it reads a sequence of items
// from arg.In and passes them as arguments to "command args...".
func (x *XargsFilter) RunFilter(arg Arg) error {
	items := append([]string(nil), x.args...)
	added := 0 // Bytes added to items since last execution.
	for s := range arg.In {
		if len(items) > len(x.args) {
			// See if we have hit a byte or arg limit.
			if len(items)-len(x.args) >= x.limitArgs ||
				added+1+len(s) >= x.limitBytes {
				err := runCommand(arg, x.command, items...)
				if err != nil {
					return err
				}
				items = items[0:len(x.args)]
				added = 0
			}
		}
		items = append(items, s)
		added += 1 + len(s)
	}
	if len(items) > len(x.args) {
		return runCommand(arg, x.command, items...)
	}
	return nil
}

func runCommand(arg Arg, command string, args ...string) error {
	cmd := exec.Command(command, args...)
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := splitIntoLines(output, arg); err != nil {
		cmd.Wait()
		return err
	}
	return cmd.Wait()
}

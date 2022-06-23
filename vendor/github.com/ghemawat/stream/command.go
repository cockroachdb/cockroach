package stream

import (
	"fmt"
	"os/exec"
	"sync"
)

// Command executes "command args...".
//
// The filter's input items are fed as standard input to the command,
// one line per input item. The standard output of the command is
// split into lines and the lines form the output of the filter (with
// trailing newlines removed).
func Command(command string, args ...string) Filter {
	return FilterFunc(func(arg Arg) error {
		cmd := exec.Command(command, args...)
		input, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		output, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		var ierr error // Records error writing to command input
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range arg.In {
				_, ierr = fmt.Fprintln(input, s)
				if ierr != nil {
					input.Close()
					return
				}
			}
			ierr = input.Close()
		}()
		if err := splitIntoLines(output, arg); err != nil {
			wg.Wait()
			cmd.Wait()
			return err
		}
		err = cmd.Wait()
		wg.Wait()
		if err != nil {
			return err
		}
		return ierr
	})
}

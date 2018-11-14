package ssh

import (
	"fmt"
	"regexp"
	"strings"
)

const shellMetachars = "|&;()<> \t\n$\\`"

func Escape1(arg string) string {
	if strings.ContainsAny(arg, shellMetachars) {
		// Argument contains shell metacharacters. Double quote the
		// argument, and backslash-escape any characters that still have
		// meaning inside of double quotes.
		e := regexp.MustCompile("([$`\"\\\\])").ReplaceAllString(arg, `\$1`)
		return fmt.Sprintf(`"%s"`, e)
	}
	return arg
}

func Escape(args []string) string {
	escaped := make([]string, len(args))
	for i := range args {
		escaped[i] = Escape1(args[i])
	}
	return strings.Join(escaped, " ")
}

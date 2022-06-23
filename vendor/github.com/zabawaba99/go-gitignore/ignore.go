package gitignore

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const dblAsterisks = "**"

// Match matches patterns in the same manner that gitignore does.
// Reference https://git-scm.com/docs/gitignore.
func Match(pattern, value string) bool {
	// A blank line matches no files, so it can serve as a separator for readability.
	if pattern == "" {
		return false
	}

	// A line starting with # serves as a comment. Put a backslash ("\") in front of the first hash for patterns that begin with a hash.
	if strings.HasPrefix(pattern, "#") {
		return false
	}

	// Trailing spaces are ignored unless they are quoted with backslash ("\").
	pattern = strings.TrimSuffix(pattern, " ")

	// An optional prefix "!" which negates the pattern; any matching file
	// excluded by a previous pattern will become included again. It is not
	// possible to re-include a file if a parent directory of that file is excluded.
	// Git doesn’t list excluded directories for performance reasons, so any patterns
	// on contained files have no effect, no matter where they are defined.
	// Put a backslash ("\") in front of the first "!" for patterns that begin
	// with a literal "!", for example, "\!important!.txt".
	negate := strings.HasPrefix(pattern, "!")
	if negate {
		pattern = strings.TrimPrefix(pattern, "!")
	}

	// If the pattern ends with a slash, it is removed for the purpose of the
	// following description, but it would only find a match with a directory.
	// In other words, foo/ will match a directory foo and paths underneath it,
	// but will not match a regular file or a symbolic link foo (this is consistent
	// with the way how pathspec works in general in Git).
	pattern = strings.TrimSuffix(pattern, string(os.PathSeparator))

	// Two consecutive asterisks ("**") in patterns matched
	// against full pathname may have special meaning:
	if strings.Contains(pattern, dblAsterisks) {
		result := evalDblAsterisk(pattern, value)
		if negate {
			result = !result
		}
		return result
	}

	// If the pattern does not contain a slash /, Git treats it as a shell glob
	// pattern and checks for a match against the pathname relative to the location
	// of the .gitignore file (relative to the toplevel of the work tree if not from
	// a .gitignore file).
	if !strings.Contains(pattern, string(os.PathSeparator)) {
		m, err := filepath.Glob(pattern)
		if err != nil {
			// maybe log this?
			log.Printf("ERROR %s\n", err)
			return false
		}

		var found bool
		for _, v := range m {
			if v == value {
				found = true
				break
			}
		}

		if negate {
			return !found
		}
		return found
	}

	// Otherwise, Git treats the pattern as a shell glob suitable for consumption by
	// fnmatch(3) with the FNM_PATHNAME flag: wildcards in the pattern will not match
	// a / in the pathname. For example, "Documentation/*.html" matches
	// "Documentation/git.html" but not "Documentation/ppc/ppc.html" or
	// "tools/perf/Documentation/perf.html".

	// A leading slash matches the beginning of the pathname. For example, "/*.c" matches "cat-file.c" but not "mozilla-sha1/sha1.c".

	matched, err := path.Match(pattern, value)
	if err != nil {
		// maybe log?
		return false
	}

	if negate {
		return !matched
	}
	return matched
}

func evalDblAsterisk(pattern, value string) bool {
	// A leading "**" followed by a slash means match in all directories.
	// For example, "**/foo" matches file or directory "foo" anywhere,
	// the same as pattern "foo". "**/foo/bar" matches file or directory
	// "bar" anywhere that is directly under directory "foo".
	if strings.HasPrefix(pattern, dblAsterisks) {
		pattern = strings.TrimPrefix(pattern, dblAsterisks)
		return strings.HasSuffix(value, pattern)
	}

	// A trailing "/**" matches everything inside. For example, "abc/**"
	// matches all files inside directory "abc", relative to the location
	// of the .gitignore file, with infinite depth.
	if strings.HasSuffix(pattern, dblAsterisks) {
		pattern = strings.TrimSuffix(pattern, dblAsterisks)
		return strings.HasPrefix(value, pattern)
	}

	// A slash followed by two consecutive asterisks then a slash matches
	// zero or more directories. For example, "a/**/b" matches "a/b",
	// /"a/x/b", "a/x/y/b" and so on.
	parts := strings.Split(pattern, dblAsterisks)
	for i, part := range parts {
		switch i {
		case 0:
			if !strings.HasPrefix(value, part) {
				return false
			}
		case len(parts) - 1: // last part
			part = strings.TrimPrefix(part, string(os.PathSeparator))
			return strings.HasSuffix(value, part)
		default:
			if !strings.Contains(value, part) {
				return false
			}
		}

		// trim evaluated text
		index := strings.Index(value, part) + len(part)
		value = value[index:]
	}

	// Other consecutive asterisks are considered invalid.
	return false
}

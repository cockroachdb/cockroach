package lintcmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
)

type parseBuildConfigError struct {
	line int
	err  error
}

func (err parseBuildConfigError) Error() string { return err.err.Error() }

func parseBuildConfigs(r io.Reader) ([]BuildConfig, error) {
	var builds []BuildConfig
	br := bufio.NewReader(r)
	i := 0
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, envs, flags, err := parseBuildConfig(line)
		if err != nil {
			return nil, parseBuildConfigError{line: i + 1, err: err}
		}

		bc := BuildConfig{
			Name:  name,
			Envs:  envs,
			Flags: flags,
		}
		builds = append(builds, bc)

		i++
	}
	return builds, nil
}

func parseBuildConfig(line string) (name string, envs []string, flags []string, err error) {
	if line == "" {
		return "", nil, nil, errors.New("couldn't parse empty build config")
	}
	if strings.Index(line, ":") == len(line)-1 {
		name = line[:len(line)-1]
	} else {
		idx := strings.Index(line, ": ")
		if idx == -1 {
			return name, envs, flags, errors.New("missing build name")
		}
		name = line[:idx]

		var buf []rune
		var inQuote bool
		args := &envs
		for _, r := range strings.TrimSpace(line[idx+2:]) {
			switch r {
			case ' ':
				if inQuote {
					buf = append(buf, r)
				} else if len(buf) != 0 {
					if buf[0] == '-' {
						args = &flags
					}
					*args = append(*args, string(buf))
					buf = buf[:0]
				}
			case '"':
				inQuote = !inQuote
			default:
				buf = append(buf, r)
			}
		}

		if len(buf) > 0 {
			if inQuote {
				return "", nil, nil, errors.New("unterminated quoted string")
			}
			if buf[0] == '-' {
				args = &flags
			}
			*args = append(*args, string(buf))
		}
	}

	for _, r := range name {
		if !(r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)) {
			return "", nil, nil, fmt.Errorf("invalid build name %q", name)
		}
	}
	return name, envs, flags, nil
}

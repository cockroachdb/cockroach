package parser

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type testCase struct {
	file   string
	lineno int
	input  string
	output string
}

func init() {
	_ = flag.Bool("logtostderr", false, "provided only since it's passed to our tests")
}

func iterateFiles(t *testing.T, pattern string) (ch chan testCase) {
	names, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	ch = make(chan testCase)
	go func() {
		defer close(ch)
		for _, name := range names {
			fd, err := os.OpenFile(name, os.O_RDONLY, 0)
			if err != nil {
				panic(fmt.Sprintf("Could not open file %s", name))
			}

			r := bufio.NewReader(fd)
			lineno := 0
			for {
				line, err := r.ReadString('\n')
				lines := strings.Split(strings.TrimRight(line, "\n"), "#")
				lineno++
				if err != nil {
					if err != io.EOF {
						panic(fmt.Sprintf("Error reading file %s: %s", name, err.Error()))
					}
					break
				}
				input := lines[0]
				output := ""
				if len(lines) > 1 {
					output = lines[1]
				}
				if input == "" {
					continue
				}
				ch <- testCase{name, lineno, input, output}
			}
		}
	}()
	return ch
}

func TestParse(t *testing.T) {
	for tcase := range iterateFiles(t, "*.sql") {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			out = tree.String()
		}
		if out != tcase.output {
			t.Errorf("File:%s Line:%v\n%q\n%q", tcase.file, tcase.lineno, tcase.output, out)
		}
	}
}

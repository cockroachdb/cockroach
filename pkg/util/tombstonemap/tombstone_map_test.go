package tombstonemap

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

var entryRe = regexp.MustCompile(`(\w+)-(\w+)#(\d+)`)

func parseEntry(t *testing.T, s string) entry {
	m := entryRe.FindStringSubmatch(s)
	if len(m) != 4 {
		t.Fatalf("expected 4 components, but found %d", len(m))
	}
	seq, err := strconv.Atoi(m[3])
	if err != nil {
		t.Fatal(err)
	}
	return entry{
		begin: m[1],
		end:   m[2],
		seq:   seq,
	}
}

func parseM(t *testing.T, s string) *M {
	m := New()
	for _, p := range strings.Split(s, ",") {
		e := parseEntry(t, p)
		m.Add(e.begin, e.end, e.seq)
	}
	return m
}

var getRe = regexp.MustCompile(`(\w+)#(\d+)`)

func parseGet(t *testing.T, s string) (string, int) {
	m := getRe.FindStringSubmatch(s)
	if len(m) != 3 {
		t.Fatalf("expected 3 components, but found %d", len(m))
	}
	seq, err := strconv.Atoi(m[2])
	if err != nil {
		t.Fatal(err)
	}
	return m[1], seq
}

func TestGet(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		var m *M
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "get":
				if len(d.CmdArgs) != 1 {
					t.Fatalf("expected 1 argument, but found %s", d.CmdArgs)
				}
				if d.CmdArgs[0].Key != "t" {
					t.Fatalf("expected timestamp argument, but found %s", d.CmdArgs[0])
				}
				readSeq, err := strconv.Atoi(d.CmdArgs[0].Vals[0])
				if err != nil {
					t.Fatal(err)
				}

				var results []string
				for _, p := range strings.Split(d.Input, " ") {
					key, seq := parseGet(t, p)
					if m.Get(key, seq, readSeq) {
						results = append(results, "deleted")
					} else {
						results = append(results, "alive")
					}
				}
				return strings.Join(results, " ") + "\n"

			case "init":
				m = parseM(t, d.Input)
				return m.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

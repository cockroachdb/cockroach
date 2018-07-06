package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var (
	sqlfmtLen       int
	sqlfmtUseSpaces bool
	sqlfmtTabWidth  int
)

func run() error {
	flag.Parse()
	if sqlfmtLen < 1 {
		return errors.Errorf("line length must be > 0: %d", sqlfmtLen)
	}
	if sqlfmtTabWidth < 1 {
		return errors.Errorf("tab width must be > 0: %d", sqlfmtTabWidth)
	}

	in, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	sl, err := parser.Parse(string(in))
	if err != nil {
		return err
	}
	for i, s := range sl {
		if i > 0 {
			fmt.Println(";")
		}
		fmt.Print(tree.PrettyWithOpts(s, sqlfmtLen, !sqlfmtUseSpaces, sqlfmtTabWidth))
	}
	fmt.Println()
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
		os.Exit(1)
	}
}

func init() {
	flag.IntVarP(&sqlfmtLen, "line-length", "n", tree.DefaultPrettyWidth, "target line length")
	flag.BoolVarP(&sqlfmtUseSpaces, "spaces", "s", false, "indent with spaces instead of tabs")
	flag.IntVarP(&sqlfmtTabWidth, "tab-width", "w", 4, "tab width")
}

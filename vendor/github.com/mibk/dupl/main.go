package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mibk/dupl/job"
	"github.com/mibk/dupl/output"
	"github.com/mibk/dupl/syntax"
)

const defaultThreshold = 15

var (
	paths     = []string{"."}
	vendor    = flag.Bool("vendor", false, "check files in vendor directory")
	verbose   = flag.Bool("verbose", false, "explain what is being done")
	threshold = flag.Int("threshold", defaultThreshold, "minimum token sequence as a clone")
	files     = flag.Bool("files", false, "files names from stdin")

	html     = flag.Bool("html", false, "html output")
	plumbing = flag.Bool("plumbing", false, "plumbing output for consumption by scripts or tools")
)

const (
	vendorDirPrefix = "vendor" + string(filepath.Separator)
	vendorDirInPath = string(filepath.Separator) + "vendor" + string(filepath.Separator)
)

func init() {
	flag.BoolVar(verbose, "v", false, "alias for -verbose")
	flag.IntVar(threshold, "t", defaultThreshold, "alias for -threshold")
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage of dupl:
  dupl [flags] [paths]

Paths:
  If the given path is a file, dupl will use it regardless of
  the file extension. If it is a directory it will recursively
  search for *.go files in that directory.

  If no path is given dupl will recursively search for *.go
  files in the current directory.

Flags:
  -files
    	read file names from stdin one at each line
  -html
    	output the results as HTML, including duplicate code fragments
  -plumbing
    	plumbing (easy-to-parse) output for consumption by scripts or tools
  -t, -threshold size
    	minimum token sequence size as a clone (default 15)
  -vendor
    	check files in vendor directory
  -v, -verbose
    	explain what is being done

Examples:
  dupl -t 100
    	Search clones in the current directory of size at least
    	100 tokens.
  dupl $(find app/ -name '*_test.go')
    	Search for clones in tests in the app directory.
  find app/ -name '*_test.go' |dupl -files
    	The same as above.`)
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *html && *plumbing {
		log.Fatal("you can have either plumbing or HTML output")
	}
	if flag.NArg() > 0 {
		paths = flag.Args()
	}

	if *verbose {
		log.Println("Building suffix tree")
	}
	schan := job.Parse(filesFeed())
	t, data, done := job.BuildTree(schan)
	<-done

	// finish stream
	t.Update(&syntax.Node{Type: -1})

	if *verbose {
		log.Println("Searching for clones")
	}
	mchan := t.FindDuplOver(*threshold)
	duplChan := make(chan syntax.Match)
	go func() {
		for m := range mchan {
			match := syntax.FindSyntaxUnits(*data, m, *threshold)
			if len(match.Frags) > 0 {
				duplChan <- match
			}
		}
		close(duplChan)
	}()
	printDupls(duplChan)
}

func filesFeed() chan string {
	if *files {
		fchan := make(chan string)
		go func() {
			s := bufio.NewScanner(os.Stdin)
			for s.Scan() {
				f := s.Text()
				if strings.HasPrefix(f, "./") {
					f = f[2:]
				}
				fchan <- f
			}
			close(fchan)
		}()
		return fchan
	}
	return crawlPaths(paths)
}

func crawlPaths(paths []string) chan string {
	fchan := make(chan string)
	go func() {
		for _, path := range paths {
			info, err := os.Lstat(path)
			if err != nil {
				log.Fatal(err)
			}
			if !info.IsDir() {
				fchan <- path
				continue
			}
			filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
				if !*vendor && (strings.HasPrefix(path, vendorDirPrefix) ||
					strings.Contains(path, vendorDirInPath)) {
					return nil
				}
				if !info.IsDir() && strings.HasSuffix(info.Name(), ".go") {
					fchan <- path
				}
				return nil
			})
		}
		close(fchan)
	}()
	return fchan
}

func printDupls(duplChan <-chan syntax.Match) {
	groups := make(map[string][][]*syntax.Node)
	for dupl := range duplChan {
		groups[dupl.Hash] = append(groups[dupl.Hash], dupl.Frags...)
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	p := getPrinter()
	for _, k := range keys {
		uniq := unique(groups[k])
		if len(uniq) > 1 {
			if err := p.Print(uniq); err != nil {
				log.Fatal(err)
			}
		}
	}
	p.Finish()
}

func getPrinter() output.Printer {
	var fr fileReader
	if *html {
		return output.NewHTMLPrinter(os.Stdout, fr)
	} else if *plumbing {
		return output.NewPlumbingPrinter(os.Stdout, fr)
	}
	return output.NewTextPrinter(os.Stdout, fr)
}

type fileReader struct{}

func (fileReader) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

func unique(group [][]*syntax.Node) [][]*syntax.Node {
	fileMap := make(map[string]map[int]struct{})

	var newGroup [][]*syntax.Node
	for _, seq := range group {
		node := seq[0]
		file, ok := fileMap[node.Filename]
		if !ok {
			file = make(map[int]struct{})
			fileMap[node.Filename] = file
		}
		if _, ok := file[node.Pos]; !ok {
			file[node.Pos] = struct{}{}
			newGroup = append(newGroup, seq)
		}
	}
	return newGroup
}

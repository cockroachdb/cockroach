package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/mibk/dupl/job"
	"github.com/mibk/dupl/output"
	"github.com/mibk/dupl/remote"
	"github.com/mibk/dupl/syntax"
)

const DefaultThreshold = 15

var (
	paths      = []string{"."}
	vendor     = flag.Bool("vendor", false, "check files in vendor directory")
	verbose    = flag.Bool("verbose", false, "explain what is being done")
	threshold  = flag.Int("threshold", DefaultThreshold, "minimum token sequence as a clone")
	serverPort = flag.String("serve", "", "run server at port")
	addrs      AddrList
	files      = flag.Bool("files", false, "files names from stdin")

	html     = flag.Bool("html", false, "html output")
	plumbing = flag.Bool("plumbing", false, "plumbing output for consumption by scripts or tools")
)

const (
	vendorDirPrefix = "vendor" + string(filepath.Separator)
	vendorDirInPath = string(filepath.Separator) + "vendor" + string(filepath.Separator)
)

type AddrList []string

func (l *AddrList) String() string {
	return fmt.Sprintf("%v", *l)
}

func (l *AddrList) Set(val string) error {
	*l = append(*l, val)
	return nil
}

func init() {
	flag.BoolVar(verbose, "v", false, "alias for -verbose")
	flag.IntVar(threshold, "t", DefaultThreshold, "alias for -threshold")
	flag.Var(&addrs, "connect", "connect to the given 'addr:port'")
	flag.Var(&addrs, "c", "alias for -connect")
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
  -c, -connect addr:port
    	connect to the given 'addr:port'
  -files
    	read file names from stdin one at each line
  -html
    	output the results as HTML, including duplicate code fragments
  -plumbing
    	plumbing (easy-to-parse) output for consumption by scripts or tools
  -serve port
    	run server at port
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

	if len(addrs) != 0 {
		schan := job.Parse(FilesFeed())
		nodesChan := remote.RunClient(addrs, *threshold, schan, *verbose)
		printDupls(nodesChan)
	} else if *serverPort != "" {
		remote.RunServer(*serverPort)
	} else {
		if *verbose {
			log.Println("Building suffix tree")
		}
		schan := job.Parse(FilesFeed())
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
}

func FilesFeed() chan string {
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
	return CrawlPaths(paths)
}

func CrawlPaths(paths []string) chan string {
	fchan := make(chan string)
	go func() {
		for _, path := range paths {
			info, err := os.Lstat(path)
			if err != nil {
				panic(err)
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

type LocalFileReader struct{}

func (r *LocalFileReader) ReadFile(node *syntax.Node) ([]byte, error) {
	return ioutil.ReadFile(node.Filename)
}

func printDupls(duplChan <-chan syntax.Match) {
	groups := make(map[string][][]*syntax.Node)
	for dupl := range duplChan {
		hash := dupl.Hash
		if _, ok := groups[hash]; ok {
			groups[hash] = append(groups[hash], dupl.Frags...)
		} else {
			groups[hash] = dupl.Frags
		}
	}

	p := getPrinter()
	for _, group := range groups {
		uniq := Unique(group)
		if len(uniq) != 1 {
			p.Print(uniq)
		}
	}
	p.Finish()
}

func getPrinter() output.Printer {
	fr := new(LocalFileReader)
	if *html {
		return output.NewHtmlPrinter(os.Stdout, fr)
	} else if *plumbing {
		return output.NewPlumbingPrinter(os.Stdout, fr)
	}
	return output.NewTextPrinter(os.Stdout, fr)
}

func Unique(group [][]*syntax.Node) [][]*syntax.Node {
	fileMap := make(map[string]map[int]bool)

	newGroup := make([][]*syntax.Node, 0)
	for _, seq := range group {
		node := seq[0]
		file, ok := fileMap[node.Filename]
		if !ok {
			file = make(map[int]bool)
			fileMap[node.Filename] = file
		}
		if _, ok = file[node.Pos]; !ok {
			file[node.Pos] = true
			newGroup = append(newGroup, seq)
		}
	}
	return newGroup
}

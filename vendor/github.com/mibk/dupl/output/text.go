package output

import (
	"fmt"
	"io"
	"sort"

	"github.com/mibk/dupl/syntax"
)

type FileReader interface {
	ReadFile(filename string) ([]byte, error)
}

type Printer interface {
	Print(dups [][]*syntax.Node) error
	Finish()
}

type TextPrinter struct {
	writer  io.Writer
	freader FileReader
	cnt     int
}

func NewTextPrinter(w io.Writer, fr FileReader) *TextPrinter {
	return &TextPrinter{
		writer:  w,
		freader: fr,
	}
}

func (p *TextPrinter) Print(dups [][]*syntax.Node) error {
	p.cnt++
	fmt.Fprintf(p.writer, "found %d clones:\n", len(dups))
	clones, err := p.prepareClonesInfo(dups)
	if err != nil {
		return err
	}
	sort.Sort(byNameAndLine(clones))
	for _, cl := range clones {
		fmt.Fprintf(p.writer, "  %s:%d,%d\n", cl.filename, cl.lineStart, cl.lineEnd)
	}
	return nil
}

func (p *TextPrinter) prepareClonesInfo(dups [][]*syntax.Node) ([]clone, error) {
	clones := make([]clone, len(dups))
	for i, dup := range dups {
		cnt := len(dup)
		if cnt == 0 {
			panic("zero length dup")
		}
		nstart := dup[0]
		nend := dup[cnt-1]

		file, err := p.freader.ReadFile(nstart.Filename)
		if err != nil {
			return nil, err
		}

		cl := clone{filename: nstart.Filename}
		cl.lineStart, cl.lineEnd = blockLines(file, nstart.Pos, nend.End)
		clones[i] = cl
	}
	return clones, nil
}

func (p *TextPrinter) Finish() {
	fmt.Fprintf(p.writer, "\nFound total %d clone groups.\n", p.cnt)
}

func blockLines(file []byte, from, to int) (int, int) {
	line := 1
	lineStart, lineEnd := 0, 0
	for offset, b := range file {
		if b == '\n' {
			line++
		}
		if offset == from {
			lineStart = line
		}
		if offset == to-1 {
			lineEnd = line
			break
		}
	}
	return lineStart, lineEnd
}

type clone struct {
	filename  string
	lineStart int
	lineEnd   int
	fragment  []byte
}

type byNameAndLine []clone

func (c byNameAndLine) Len() int { return len(c) }

func (c byNameAndLine) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func (c byNameAndLine) Less(i, j int) bool {
	if c[i].filename == c[j].filename {
		return c[i].lineStart < c[j].lineStart
	}
	return c[i].filename < c[j].filename
}

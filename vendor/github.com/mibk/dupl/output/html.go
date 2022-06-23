package output

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"sort"

	"github.com/mibk/dupl/syntax"
)

type HTMLPrinter struct {
	iota int
	*TextPrinter
}

func NewHTMLPrinter(w io.Writer, fr FileReader) *HTMLPrinter {
	fmt.Fprint(w, `<!DOCTYPE html>
<meta charset="utf-8"/>
<title>Duplicates</title>
<style>
	pre {
		background-color: #FFD;
		border: 1px solid #E2E2E2;
		padding: 1ex;
	}
</style>
`)
	return &HTMLPrinter{
		TextPrinter: NewTextPrinter(w, fr),
	}
}

func (p *HTMLPrinter) Print(dups [][]*syntax.Node) error {
	p.iota++
	fmt.Fprintf(p.writer, "<h1>#%d found %d clones</h1>\n", p.iota, len(dups))

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
			return err
		}

		lineStart, _ := blockLines(file, nstart.Pos, nend.End)
		cl := clone{filename: nstart.Filename, lineStart: lineStart}
		start := findLineBeg(file, nstart.Pos)
		content := append(toWhitespace(file[start:nstart.Pos]), file[nstart.Pos:nend.End]...)
		cl.fragment = deindent(content)
		clones[i] = cl
	}

	sort.Sort(byNameAndLine(clones))
	for _, cl := range clones {
		fmt.Fprintf(p.writer, "<h2>%s:%d</h2>\n<pre>%s</pre>\n", cl.filename, cl.lineStart, cl.fragment)
	}
	return nil
}

func (*HTMLPrinter) Finish() {}

func findLineBeg(file []byte, index int) int {
	for i := index; i >= 0; i-- {
		if file[i] == '\n' {
			return i + 1
		}
	}
	return 0
}

func toWhitespace(str []byte) []byte {
	var out []byte
	for _, c := range bytes.Runes(str) {
		if c == '\t' {
			out = append(out, '\t')
		} else {
			out = append(out, ' ')
		}
	}
	return out
}

func deindent(block []byte) []byte {
	const maxVal = 99
	min := maxVal
	re := regexp.MustCompile(`(^|\n)(\t*)\S`)
	for _, line := range re.FindAllSubmatch(block, -1) {
		indent := line[2]
		if len(indent) < min {
			min = len(indent)
		}
	}
	if min == 0 || min == maxVal {
		return block
	}
	block = block[min:]
Loop:
	for i := 0; i < len(block); i++ {
		if block[i] == '\n' && i != len(block)-1 {
			for j := 0; j < min; j++ {
				if block[i+j+1] != '\t' {
					continue Loop
				}
			}
			block = append(block[:i+1], block[i+1+min:]...)
		}
	}
	return block
}

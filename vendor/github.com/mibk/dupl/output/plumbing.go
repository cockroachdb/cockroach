package output

import (
	"fmt"
	"io"
	"sort"

	"github.com/mibk/dupl/syntax"
)

type PlumbingPrinter struct {
	*TextPrinter
}

func NewPlumbingPrinter(w io.Writer, fr FileReader) *PlumbingPrinter {
	return &PlumbingPrinter{NewTextPrinter(w, fr)}
}

func (p *PlumbingPrinter) Print(dups [][]*syntax.Node) {
	clones := p.prepareClonesInfo(dups)
	sort.Sort(byNameAndLine(clones))
	for i, cl := range clones {
		nextCl := clones[(i+1)%len(clones)]
		fmt.Fprintf(p.writer, "%s:%d-%d: duplicate of %s:%d-%d\n", cl.filename, cl.lineStart, cl.lineEnd,
			nextCl.filename, nextCl.lineStart, nextCl.lineEnd)
	}
}

func (p *PlumbingPrinter) Finish() {}

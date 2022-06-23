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

func (p *PlumbingPrinter) Print(dups [][]*syntax.Node) error {
	clones, err := p.prepareClonesInfo(dups)
	if err != nil {
		return err
	}
	sort.Sort(byNameAndLine(clones))
	for i, cl := range clones {
		nextCl := clones[(i+1)%len(clones)]
		fmt.Fprintf(p.writer, "%s:%d-%d: duplicate of %s:%d-%d\n", cl.filename, cl.lineStart, cl.lineEnd,
			nextCl.filename, nextCl.lineStart, nextCl.lineEnd)
	}
	return nil
}

func (p *PlumbingPrinter) Finish() {}

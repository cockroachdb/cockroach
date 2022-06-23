package dot

import (
	"fmt"
	"io"
)

type IndentWriter struct {
	level  int
	writer io.Writer
}

func NewIndentWriter(w io.Writer) *IndentWriter {
	return &IndentWriter{level: 0, writer: w}
}

func (i *IndentWriter) Indent() {
	i.level++
	fmt.Fprint(i.writer, "\t")
}

func (i *IndentWriter) BackIndent() {
	i.level--
}

func (i *IndentWriter) IndentWhile(block func()) {
	i.Indent()
	block()
	i.BackIndent()
}

func (i *IndentWriter) NewLineIndentWhile(block func()) {
	i.NewLine()
	i.Indent()
	block()
	i.BackIndent()
	i.NewLine()
}

func (i *IndentWriter) NewLine() {
	fmt.Fprint(i.writer, "\n")
	for j := 0; j < i.level; j++ {
		fmt.Fprint(i.writer, "\t")
	}
}

// Write makes it an io.Writer
func (i *IndentWriter) Write(data []byte) (n int, err error) {
	return i.writer.Write(data)
}

func (i *IndentWriter) WriteString(s string) (n int, err error) {
	fmt.Fprint(i.writer, s)
	return len(s), nil
}

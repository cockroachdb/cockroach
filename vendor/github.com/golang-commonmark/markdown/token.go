// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

type Token interface {
	Tag() string
	Opening() bool
	Closing() bool
	Block() bool
	Level() int
	SetLevel(lvl int)
}

type BlockquoteOpen struct {
	Map [2]int
	Lvl int
}

type BlockquoteClose struct {
	Lvl int
}

type BulletListOpen struct {
	Map [2]int
	Lvl int
}

type BulletListClose struct {
	Lvl int
}

type OrderedListOpen struct {
	Order int
	Map   [2]int
	Lvl   int
}

type OrderedListClose struct {
	Lvl int
}

type ListItemOpen struct {
	Map [2]int
	Lvl int
}

type ListItemClose struct {
	Lvl int
}

type CodeBlock struct {
	Content string
	Map     [2]int
	Lvl     int
}

type CodeInline struct {
	Content string
	Lvl     int
}

type EmphasisOpen struct {
	Lvl int
}

type EmphasisClose struct {
	Lvl int
}

type StrongOpen struct {
	Lvl int
}

type StrongClose struct {
	Lvl int
}

type StrikethroughOpen struct {
	Lvl int
}

type StrikethroughClose struct {
	Lvl int
}

type Fence struct {
	Params  string
	Content string
	Map     [2]int
	Lvl     int
}

type Softbreak struct {
	Lvl int
}

type Hardbreak struct {
	Lvl int
}

type HeadingOpen struct {
	HLevel int
	Map    [2]int
	Lvl    int
}

type HeadingClose struct {
	HLevel int
	Lvl    int
}

type HTMLBlock struct {
	Content string
	Map     [2]int
	Lvl     int
}

type HTMLInline struct {
	Content string
	Lvl     int
}

type Hr struct {
	Map [2]int
	Lvl int
}

type Image struct {
	Src    string
	Title  string
	Tokens []Token
	Lvl    int
}

type Inline struct {
	Content  string
	Map      [2]int
	Children []Token
	Lvl      int
}

type LinkOpen struct {
	Href   string
	Title  string
	Target string
	Lvl    int
}

type LinkClose struct {
	Lvl int
}

type ParagraphOpen struct {
	Tight  bool
	Hidden bool
	Map    [2]int
	Lvl    int
}

type ParagraphClose struct {
	Tight  bool
	Hidden bool
	Lvl    int
}

type TableOpen struct {
	Map [2]int
	Lvl int
}

type TableClose struct {
	Lvl int
}

type TheadOpen struct {
	Map [2]int
	Lvl int
}

type TheadClose struct {
	Lvl int
}

type TrOpen struct {
	Map [2]int
	Lvl int
}

type TrClose struct {
	Lvl int
}

type ThOpen struct {
	Align Align
	Map   [2]int
	Lvl   int
}

type ThClose struct {
	Lvl int
}

type TbodyOpen struct {
	Map [2]int
	Lvl int
}

type TbodyClose struct {
	Lvl int
}

type TdOpen struct {
	Align Align
	Map   [2]int
	Lvl   int
}

type TdClose struct {
	Lvl int
}

type Text struct {
	Content string
	Lvl     int
}

var htags = []string{
	"",
	"h1",
	"h2",
	"h3",
	"h4",
	"h5",
	"h6",
}

func (t *BlockquoteOpen) Level() int { return t.Lvl }

func (t *BlockquoteClose) Level() int { return t.Lvl }

func (t *BulletListOpen) Level() int { return t.Lvl }

func (t *BulletListClose) Level() int { return t.Lvl }

func (t *OrderedListOpen) Level() int { return t.Lvl }

func (t *OrderedListClose) Level() int { return t.Lvl }

func (t *ListItemOpen) Level() int { return t.Lvl }

func (t *ListItemClose) Level() int { return t.Lvl }

func (t *CodeBlock) Level() int { return t.Lvl }

func (t *CodeInline) Level() int { return t.Lvl }

func (t *EmphasisOpen) Level() int { return t.Lvl }

func (t *EmphasisClose) Level() int { return t.Lvl }

func (t *StrongOpen) Level() int { return t.Lvl }

func (t *StrongClose) Level() int { return t.Lvl }

func (t *StrikethroughOpen) Level() int { return t.Lvl }

func (t *StrikethroughClose) Level() int { return t.Lvl }

func (t *Fence) Level() int { return t.Lvl }

func (t *Softbreak) Level() int { return t.Lvl }

func (t *Hardbreak) Level() int { return t.Lvl }

func (t *HeadingOpen) Level() int { return t.Lvl }

func (t *HeadingClose) Level() int { return t.Lvl }

func (t *HTMLBlock) Level() int { return t.Lvl }

func (t *HTMLInline) Level() int { return t.Lvl }

func (t *Hr) Level() int { return t.Lvl }

func (t *Image) Level() int { return t.Lvl }

func (t *Inline) Level() int { return t.Lvl }

func (t *LinkOpen) Level() int { return t.Lvl }

func (t *LinkClose) Level() int { return t.Lvl }

func (t *ParagraphOpen) Level() int { return t.Lvl }

func (t *ParagraphClose) Level() int { return t.Lvl }

func (t *TableOpen) Level() int { return t.Lvl }

func (t *TableClose) Level() int { return t.Lvl }

func (t *TheadOpen) Level() int { return t.Lvl }

func (t *TheadClose) Level() int { return t.Lvl }

func (t *TrOpen) Level() int { return t.Lvl }

func (t *TrClose) Level() int { return t.Lvl }

func (t *ThOpen) Level() int { return t.Lvl }

func (t *ThClose) Level() int { return t.Lvl }

func (t *TbodyOpen) Level() int { return t.Lvl }

func (t *TbodyClose) Level() int { return t.Lvl }

func (t *TdOpen) Level() int { return t.Lvl }

func (t *TdClose) Level() int { return t.Lvl }

func (t *Text) Level() int { return t.Lvl }

func (t *BlockquoteOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *BlockquoteClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *BulletListOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *BulletListClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *OrderedListOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *OrderedListClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *ListItemOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *ListItemClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *CodeBlock) SetLevel(lvl int) { t.Lvl = lvl }

func (t *CodeInline) SetLevel(lvl int) { t.Lvl = lvl }

func (t *EmphasisOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *EmphasisClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *StrongOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *StrongClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *StrikethroughOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *StrikethroughClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *Fence) SetLevel(lvl int) { t.Lvl = lvl }

func (t *Softbreak) SetLevel(lvl int) { t.Lvl = lvl }

func (t *Hardbreak) SetLevel(lvl int) { t.Lvl = lvl }

func (t *HeadingOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *HeadingClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *HTMLBlock) SetLevel(lvl int) { t.Lvl = lvl }

func (t *HTMLInline) SetLevel(lvl int) { t.Lvl = lvl }

func (t *Hr) SetLevel(lvl int) { t.Lvl = lvl }

func (t *Image) SetLevel(lvl int) { t.Lvl = lvl }

func (t *Inline) SetLevel(lvl int) { t.Lvl = lvl }

func (t *LinkOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *LinkClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *ParagraphOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *ParagraphClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TableOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TableClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TheadOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TheadClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TrOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TrClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *ThOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *ThClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TbodyOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TbodyClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TdOpen) SetLevel(lvl int) { t.Lvl = lvl }

func (t *TdClose) SetLevel(lvl int) { t.Lvl = lvl }

func (t *Text) SetLevel(lvl int) { t.Lvl = lvl }

func (t *BlockquoteOpen) Opening() bool { return true }

func (t *BlockquoteClose) Opening() bool { return false }

func (t *BulletListOpen) Opening() bool { return true }

func (t *BulletListClose) Opening() bool { return false }

func (t *OrderedListOpen) Opening() bool { return true }

func (t *OrderedListClose) Opening() bool { return false }

func (t *ListItemOpen) Opening() bool { return true }

func (t *ListItemClose) Opening() bool { return false }

func (t *CodeBlock) Opening() bool { return false }

func (t *CodeInline) Opening() bool { return false }

func (t *EmphasisOpen) Opening() bool { return true }

func (t *EmphasisClose) Opening() bool { return false }

func (t *StrongOpen) Opening() bool { return true }

func (t *StrongClose) Opening() bool { return false }

func (t *StrikethroughOpen) Opening() bool { return true }

func (t *StrikethroughClose) Opening() bool { return false }

func (t *Fence) Opening() bool { return false }

func (t *Softbreak) Opening() bool { return false }

func (t *Hardbreak) Opening() bool { return false }

func (t *HeadingOpen) Opening() bool { return true }

func (t *HeadingClose) Opening() bool { return false }

func (t *HTMLBlock) Opening() bool { return false }

func (t *HTMLInline) Opening() bool { return false }

func (t *Hr) Opening() bool { return false }

func (t *Image) Opening() bool { return false }

func (t *Inline) Opening() bool { return false }

func (t *LinkOpen) Opening() bool { return true }

func (t *LinkClose) Opening() bool { return false }

func (t *ParagraphOpen) Opening() bool { return true }

func (t *ParagraphClose) Opening() bool { return false }

func (t *TableOpen) Opening() bool { return true }

func (t *TableClose) Opening() bool { return false }

func (t *TheadOpen) Opening() bool { return true }

func (t *TheadClose) Opening() bool { return false }

func (t *TrOpen) Opening() bool { return true }

func (t *TrClose) Opening() bool { return false }

func (t *ThOpen) Opening() bool { return true }

func (t *ThClose) Opening() bool { return false }

func (t *TbodyOpen) Opening() bool { return true }

func (t *TbodyClose) Opening() bool { return false }

func (t *TdOpen) Opening() bool { return true }

func (t *TdClose) Opening() bool { return false }

func (t *Text) Opening() bool { return false }

func (t *BlockquoteOpen) Closing() bool { return false }

func (t *BlockquoteClose) Closing() bool { return true }

func (t *BulletListOpen) Closing() bool { return false }

func (t *BulletListClose) Closing() bool { return true }

func (t *OrderedListOpen) Closing() bool { return false }

func (t *OrderedListClose) Closing() bool { return true }

func (t *ListItemOpen) Closing() bool { return false }

func (t *ListItemClose) Closing() bool { return true }

func (t *CodeBlock) Closing() bool { return false }

func (t *CodeInline) Closing() bool { return false }

func (t *EmphasisOpen) Closing() bool { return false }

func (t *EmphasisClose) Closing() bool { return true }

func (t *StrongOpen) Closing() bool { return false }

func (t *StrongClose) Closing() bool { return true }

func (t *StrikethroughOpen) Closing() bool { return false }

func (t *StrikethroughClose) Closing() bool { return true }

func (t *Fence) Closing() bool { return false }

func (t *Softbreak) Closing() bool { return false }

func (t *Hardbreak) Closing() bool { return false }

func (t *HeadingOpen) Closing() bool { return false }

func (t *HeadingClose) Closing() bool { return true }

func (t *HTMLBlock) Closing() bool { return false }

func (t *HTMLInline) Closing() bool { return false }

func (t *Hr) Closing() bool { return false }

func (t *Image) Closing() bool { return false }

func (t *Inline) Closing() bool { return false }

func (t *LinkOpen) Closing() bool { return false }

func (t *LinkClose) Closing() bool { return true }

func (t *ParagraphOpen) Closing() bool { return false }

func (t *ParagraphClose) Closing() bool { return true }

func (t *TableOpen) Closing() bool { return false }

func (t *TableClose) Closing() bool { return true }

func (t *TheadOpen) Closing() bool { return false }

func (t *TheadClose) Closing() bool { return true }

func (t *TrOpen) Closing() bool { return false }

func (t *TrClose) Closing() bool { return true }

func (t *ThOpen) Closing() bool { return false }

func (t *ThClose) Closing() bool { return true }

func (t *TbodyOpen) Closing() bool { return false }

func (t *TbodyClose) Closing() bool { return true }

func (t *TdOpen) Closing() bool { return false }

func (t *TdClose) Closing() bool { return true }

func (t *Text) Closing() bool { return false }

func (t *BlockquoteOpen) Block() bool { return true }

func (t *BlockquoteClose) Block() bool { return true }

func (t *BulletListOpen) Block() bool { return true }

func (t *BulletListClose) Block() bool { return true }

func (t *OrderedListOpen) Block() bool { return true }

func (t *OrderedListClose) Block() bool { return true }

func (t *ListItemOpen) Block() bool { return true }

func (t *ListItemClose) Block() bool { return true }

func (t *CodeBlock) Block() bool { return true }

func (t *CodeInline) Block() bool { return false }

func (t *EmphasisOpen) Block() bool { return false }

func (t *EmphasisClose) Block() bool { return false }

func (t *StrongOpen) Block() bool { return false }

func (t *StrongClose) Block() bool { return false }

func (t *StrikethroughOpen) Block() bool { return false }

func (t *StrikethroughClose) Block() bool { return false }

func (t *Fence) Block() bool { return true }

func (t *Softbreak) Block() bool { return false }

func (t *Hardbreak) Block() bool { return false }

func (t *HeadingOpen) Block() bool { return true }

func (t *HeadingClose) Block() bool { return true }

func (t *HTMLBlock) Block() bool { return true }

func (t *HTMLInline) Block() bool { return false }

func (t *Hr) Block() bool { return true }

func (t *Image) Block() bool { return false }

func (t *Inline) Block() bool { return false }

func (t *LinkOpen) Block() bool { return false }

func (t *LinkClose) Block() bool { return false }

func (t *ParagraphOpen) Block() bool { return true }

func (t *ParagraphClose) Block() bool { return true }

func (t *TableOpen) Block() bool { return true }

func (t *TableClose) Block() bool { return true }

func (t *TheadOpen) Block() bool { return true }

func (t *TheadClose) Block() bool { return true }

func (t *TrOpen) Block() bool { return true }

func (t *TrClose) Block() bool { return true }

func (t *ThOpen) Block() bool { return true }

func (t *ThClose) Block() bool { return true }

func (t *TbodyOpen) Block() bool { return true }

func (t *TbodyClose) Block() bool { return true }

func (t *TdOpen) Block() bool { return true }

func (t *TdClose) Block() bool { return true }

func (t *Text) Block() bool { return false }

func (t *BlockquoteOpen) Tag() string { return "blockquote" }

func (t *BlockquoteClose) Tag() string { return "blockquote" }

func (t *BulletListOpen) Tag() string { return "ul" }

func (t *BulletListClose) Tag() string { return "ul" }

func (t *OrderedListOpen) Tag() string { return "ol" }

func (t *OrderedListClose) Tag() string { return "ol" }

func (t *ListItemOpen) Tag() string { return "li" }

func (t *ListItemClose) Tag() string { return "li" }

func (t *CodeBlock) Tag() string { return "code" }

func (t *CodeInline) Tag() string { return "code" }

func (t *EmphasisOpen) Tag() string { return "em" }

func (t *EmphasisClose) Tag() string { return "em" }

func (t *StrongOpen) Tag() string { return "strong" }

func (t *StrongClose) Tag() string { return "strong" }

func (t *StrikethroughOpen) Tag() string { return "s" }

func (t *StrikethroughClose) Tag() string { return "s" }

func (t *Fence) Tag() string { return "code" }

func (t *Softbreak) Tag() string { return "br" }

func (t *Hardbreak) Tag() string { return "br" }

func (t *HeadingOpen) Tag() string { return htags[t.HLevel] }

func (t *HeadingClose) Tag() string { return htags[t.HLevel] }

func (t *HTMLBlock) Tag() string { return "" }

func (t *HTMLInline) Tag() string { return "" }

func (t *Hr) Tag() string { return "hr" }

func (t *Image) Tag() string { return "img" }

func (t *Inline) Tag() string { return "" }

func (t *LinkOpen) Tag() string { return "a" }

func (t *LinkClose) Tag() string { return "a" }

func (t *ParagraphOpen) Tag() string { return "p" }

func (t *ParagraphClose) Tag() string { return "p" }

func (t *TableOpen) Tag() string { return "table" }

func (t *TableClose) Tag() string { return "table" }

func (t *TheadOpen) Tag() string { return "thead" }

func (t *TheadClose) Tag() string { return "thead" }

func (t *TrOpen) Tag() string { return "tr" }

func (t *TrClose) Tag() string { return "tr" }

func (t *ThOpen) Tag() string { return "th" }

func (t *ThClose) Tag() string { return "th" }

func (t *TbodyOpen) Tag() string { return "tbody" }

func (t *TbodyClose) Tag() string { return "tbody" }

func (t *TdOpen) Tag() string { return "td" }

func (t *TdClose) Tag() string { return "td" }

func (t *Text) Tag() string { return "" }

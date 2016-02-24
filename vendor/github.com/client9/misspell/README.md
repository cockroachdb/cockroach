[![Build Status](https://travis-ci.org/client9/misspell.svg?branch=master)](https://travis-ci.org/client9/misspell) [![Go Report Card](http://goreportcard.com/badge/client9/misspell)](http://goreportcard.com/report/client9/misspell) [![GoDoc](https://godoc.org/github.com/client9/misspell?status.svg)](https://godoc.org/github.com/client9/misspell) [![Coverage](http://gocover.io/_badge/github.com/client9/misspell)](http://gocover.io/github.com/client9/misspell) [![license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](https://raw.githubusercontent.com/client9/misspell/master/LICENSE)

Correct commonly misspelled English words... quickly.

### install with `go get -u github.com/client9/misspell/cmd/misspell`

new as of 2016-01-12

```bash
$ misspell all.html your.txt important.md files.go
your.txt:42:10 found "initialised" a misspelling of "initialized"

# ^ file, line, column
```

You'll need [golang 1.5](https://golang.org/) installed to compile it.  But after that it's a standalone binary.

If people want pre-compiled binaries, [file a ticket](https://github.com/client9/misspell/issues) please.

## FAQ

### How do you check an entire folder recursively?

You can run misspell recursively using the following notation:

```bash
misspell directory/**/*
```

or

```bash
find . -name '*' | xargs misspell
```
### Can I use pipes or `stdin` for input?

Yes!

Print messages to `stderr` only:

```bash
$ echo "zeebra" | misspell
stdin:1:0:found "zeebra" a misspelling of "zebra"
```

Print messages to `stderr`, and corrected text to `stdout`:

```bash
$ echo "zeebra" | misspell -w
stdin:1:0:corrected "zeebra" to "zebra"
zebra
```

Only print the corrected text to `stdout`:

```bash
$ echo "zeebra" | misspell -w -q
zebra
```

### Are there special rules for golang source files?

Yes!  If the file ends in `.go`, then misspell will only check spelling in comments.

If you want to force a file to be checked as a golang source, use
`-source=go` on the command line.  Conversely, you can check a golang
source as if it were pure text by using `-source=text`.  You might want to do this since
many variable names have misspellings in them!

### Can I check only-comments in other other programming languages?

I'm told the using `-source=go` works well for ruby, javascript, java, c and c++.

It doesn't work well for python and bash.

### Does this work with gometalinter?

[gometalinter](https://github.com/alecthomas/gometalinter) runs
multiple golang linters, and it works well with `misspell` too.

After `go get -u github.com/client9/misspell` you need to add it, then
enable it, like so:

```bash
gometalinter --disable-all \
   --linter='misspell:misspell ./*.go:PATH:LINE:COL:MESSAGE' --enable=misspell \
   ./...
```

### How can I change the output format?

Using the `-f template` flag you can pass in a
[golang text template](https://golang.org/pkg/text/template/) to format the output.

The built-in template uses everything, including the `js` function to escape the original text.

```
{{ .Filename }}:{{ .Line }}:{{ .Column }}:corrected "{{ js .Original }}" to "{{ js .Corrected }}"
```

To just print probable misspellings:

```
-f '{{ .Original }}'
```

### What problem does this solve?

This corrects commonly misspelled English words in computer source
code, and other text-based formats (`.txt`, `.md`, etc).

It is designed to run quickly so it can be
used as a [pre-commit hook](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks)
with minimal burden on the developer.

It does not work with binary formats (e.g. Word, etc).

It is not a complete spell-checking program nor a grammar checker.

### What are other misspelling correctors and what's wrong with them?

Some other misspelling correctors:

* https://github.com/vlajos/misspell_fixer
* https://github.com/lyda/misspell-check
* https://github.com/lucasdemarchi

They all work but had problems that prevented me from using them at scale:

* slow, all of the above check one misspelling at a time (i.e. linear) using regexps
* not MIT/Apache2 licensed (or equivalent)
* have dependencies that don't work for me (python3, bash, linux sed, etc)

That said, they might be perfect for you and many have more features
that this project!

### How much faster is this project?

Easily 100x to 1000x faster.  You should be able to check and correct
1000 files in under 250ms.

### What license is this?

[MIT](https://github.com/client9/misspell/blob/master/LICENSE)

### Where do the word lists come from?

It started with a word list from
[Wikipedia](https://en.wikipedia.org/wiki/Wikipedia:Lists_of_common_misspellings/For_machines)
and then edited to remove false positives.

Then additional words were added based on actually mistakes seen in
the wild.

### Why is this so fast?

This uses the mighty power of golang's
[strings.Replacer](https://golang.org/pkg/strings/#Replacer) which is
a implementation or variation of the
[Aho–Corasick algorithm](https://en.wikipedia.org/wiki/Aho–Corasick_algorithm).
This makes multiple substring matches *simultaneously*

In addition this uses multiple CPU cores to work on multiple files.

### What problems does it have?

Unlike the other projects, this doesn't know what a "word" is.  There
may be more false positives and false negatives due to this.  On the
other hand, it sometimes catches things others don't.

Either way, please file bugs and we'll fix them!

Since it operates in parallel to make corrections, it can be
non-obvious to determine exactly what word was corrected.

### It's making mistakes.  How can I debug?

Run using `-debug` flag on the file you want.  It should then
print what word it is trying to correct.  Then [file a bug](https://github.com/client9/misspell/issues) describing the
problem.  Thanks!

### Why is it making mistakes or missing items in golang files?

The matching function is *case-sensitive*, so variable names that are
multiple worlds either in all-upper or all-lower case sometimes can
cause false positives.  For instance a variable named `bodyreader`
could trigger a false positive since `yrea` is in the middle that
could be corrected to `year`.  Other problems happen if the variable
name uses a English contraction that should use an apostrophe.  The
best way of fixing this is to use the [Effective Go naming
conventions](https://golang.org/doc/effective_go.html#mixed-caps) and
use [camelCase](https://en.wikipedia.org/wiki/CamelCase) for variable names.  You can check your code using
[golint](https://github.com/golang/lint)


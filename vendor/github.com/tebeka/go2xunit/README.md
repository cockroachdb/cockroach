# go2xunit

[![Travis](https://travis-ci.org/tebeka/go2xunit.svg?branch=master)](https://travis-ci.org/tebeka/go2xunit)

Converts `go test -v` (or `gocheck -vv`) output to xunit or [xunit.net][xnet]
compatible XML output (used in [Jenkins][jenkins]/[Hudson][hudson]).


# Install

    go get bitbucket.org/tebeka/go2xunit


# Usage
By default `go2xunit` reads data from standard input and emits XML to standard
output. However you can use `-input` and `-output` flags to change this.

The `-fail` switch will cause `go2xunit` to exit with non zero status if there
are failed tests.

    go test -v | go2xunit -output tests.xml

`go2xunit` also works with [gocheck][gocheck], and [testify][testify].

    go test -gocheck.vv | go2xunit -gocheck -output tests.xml

Here's an example script (`run-tests.sh`) that can be used with [Jenkins][jenkins]/[Hudson][hudson].

    #!/bin/bash

    outfile=gotest.out

    go test -v | tee $outfile
    go2xunit -fail -input $outfile -output tests.xml


# Related

* [testing: add -json flag for json
  results](https://github.com/golang/go/issues/2981) open bug

# Contact
Miki Tebeka <miki.tebeka@gmail.com>

Bug reports go [here][bugs].


[jenkins]: http://jenkins-ci.org/
[hudson]: http://hudson-ci.org/
[gocheck]: http://labix.org/gocheck
[testify]: http://godoc.org/github.com/stretchr/testify
[bugs]: https://bitbucket.org/tebeka/go2xunit/issues
[xnet]: https://xunit.codeplex.com/wikipage?title=XmlFormat

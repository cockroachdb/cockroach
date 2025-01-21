// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goroutineui

import (
	"os"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/maruel/panicparse/v2/stack"
	"github.com/stretchr/testify/assert"
)

func init() {
	if bazel.BuiltWithBazel() {
		bazel.SetGoEnv()
	}
}

func TestDumpEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dump := NewDump()
	dump.agg = nil

	assert.NotPanics(t, func() {
		dump.SortWaitDesc()
		dump.SortCountDesc()
	})

	act := dump.HTMLString()
	assert.Contains(t, act, "goroutineui: empty goroutine dump")
}

func TestDumpHTML(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Disable some analysis passes, which are environment dependent.
	opts := stack.DefaultOpts()
	opts.GuessPaths = false
	opts.AnalyzeSources = false

	dump := newDumpFromBytes([]byte(fixture), opts)
	dump.SortWaitDesc()  // noop
	dump.SortCountDesc() // noop
	act := dump.HTMLString()

	// Mask out metadata section, which is environment and timing dependent.
	re := regexp.MustCompile(`(?ms)(Metadata</h2>.*<h2>)`)
	act = re.ReplaceAllString(act, "")

	if false {
		_ = os.WriteFile("test.html", []byte(act), 0644)
	}
	assert.Equal(t, exp, act)
}

// This is the output of debug.PrintStack() on the go playground.
const fixture = `goroutine 1 [running]:
runtime/debug.Stack(0x434070, 0xddb11, 0x0, 0x40e0f8)
	/usr/local/go/src/runtime/debug/stack.go:24 +0xc0
runtime/debug.PrintStack()
	/usr/local/go/src/runtime/debug/stack.go:16 +0x20
main.main()
	/tmp/sandbox157492124/main.go:6 +0x20`

const exp = `<!DOCTYPE html><meta charset="UTF-8">
<meta name="author" content="Marc-Antoine Ruel" >
<meta name="generator" content="https://github.com/maruel/panicparse" >
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PanicParse</title>
<link rel="shortcut icon" type="image/gif" href="data:image/gif;base64,R0lGODlhQABAAMZhACEhISIiIiMjIyQkJCUlJSYmJicnJygoKCkpKSoqKisrKywsLC0tLS4uLi8vLzAwMDExMTIyMjMzMzQ0NDU1NTY2Njc3Nzg4ODk5OTo6Ojs7Ozw8PD09PT4&#43;Pj8/P0BAQEFBQUJCQkNDQ0REREVFRUZGRkdHR4o1MEhISElJSUpKSktLS0xMTE1NTU5OTk9PT1BQUFFRUVJSUv8fQFNTU1RUVKc&#43;N1VVVVZWVldXV/8mPVhYWFlZWf8oPP8pO1paWltbW1xcXF1dXV5eXl9fX2BgYGFhYf80NWJiYv8&#43;MP9ALvRDNv9JKcxYUfRPQ/9PJvRTSP9VI/9aIPRcUf9dHudhV/9lGv91EP91Ef96Dv99C/9&#43;DP&#43;FB/&#43;MA/&#43;PAf&#43;QAP&#43;RAP///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////yH5BAEKAH8ALAAAAABAAEAAAAf&#43;gH&#43;Cg4SFhoeIiYqLjI2Oj5CRjSkjJiqSkl6YfyMXCQCgBRUkm45ahF&#43;NJRIBAAEKEhIKAAIepYVgg0uCVINXjCocAwAEGSWEHwIBI7eoXIRHgklSiyQPrhcohx0AEM2FVkqFM4sfBQALIokqBAKX34PijigVrhYpiwwAzN&#43;7g1NOmqzrYACAgRCNFgAwAa8QlCUnQLwTpGKEhgP2tAEbQGBiMyxPdPypYgPUAAYRJDA450oCv0YfAESAl6tQig4MBIACFYCBhmOKVqh4p8IBAFsNB/kgVBEEiBEeE7loYSLF0BQYJEQt5e8PE0E9MMmAocGAAQgdSgwdmvRPlkH&#43;USThoKFhgQIEBtopwAB1a6kug7ZAAsLjRgYIDxowuFtgQCutDWtCEkJZSI4NFShIiJB4AV7HAVD4bWuoiGkiOWSo6KAhAwYLmiE4WJAgbwAPo0kLKmKkSA4VIZx&#43;&#43;OCBwwbXFiZEcMCg9oAJudvyNlKDxIgRJLKPEBH8Q4fjsJc3N4CArW5CRZD8MEGihAkUKFLAN1GChAgQxTNcoLDccwES0X1jBBIwkFCVCiuwoKCCQqFQwgghfMCBfvzNdgAHQp33R28rlCAaCy68AMOIMLzwQgssqIACCRFOmBwEDSSAQF&#43;6mZaChyu08EIMMtBQw480yBDDiSus2CIGFUj&#43;8IBnBogQoCS83YjCCi7AQMMNOOSww5Y54FDDWC0UyaIHG&#43;wHY20EAJgUEUawQMKULsRAAw47/ABEEHgStsMNNMDQggomjABCBxlYIIGFBBwgWkNF3CCCJS1YiQMPQAhBBBFFEDGEEEDsMJefKpQgApkXTAABAwgUEIBWTzKyqQhvsvDCnDwEMcR0RuRaxBCd3iDDC0UKygGSEcRogE4SNRMEES2IoFYLMdywAxBD5GqttbsCkUMNfqbAIqGGOqDAAcMkYN4mnH7w6AovyIDDD0Lgeq2uQhQmgwuAjqpBckuSCwputwjhwgcjoMCClTkAwea88w7xw1zAmiDCB2X&#43;TvAAqgS00sC5kVT6AQixwlDDDssyPC8R2tLwAgsojEBxqacikDEoam3yww0cgJzCwSMHIa/JKOfQJ8sul0lBzDMDsEGrh/zQQs4hj6ywydcGrXKwL5uKcSvdMG3IDyNsQLDBsyZcLdW5OozDrytInPWSMnNtAMeO5MDDBmKv64K7PMSLdhFBFBYDvqJ6sK&#43;SC/i706KRTGqBBh6E8GwMUt9qchFC/LBtt99mkKS4Bgyzk5qR3HBDBRlATbYM0lL7MxGZ73lv2y4Py5&#43;xOu2UbCRfUnBB3s9a2bqtl2oaxA&#43;yr3xjCIaXenHcOwGwOyQ/SmBBBh08BanwdtsJBBD&#43;P/CQg68nAlq754cmLvpOTkoiQw0S&#43;L5B5G/mCMP7WOagPw43fFl&#43;oIPSAMxwFz0A1EwSNDAU6jjwgRC8SQU6uh8NJhikGMDABSliD/M2gKT0HSBpPKGbI2TAAQhM4HoMdGBVchQiE5kIgytIQaA2WCiLjSd3O1GA1wpRIAdEgAIojNwIPGSVFSTIiCqQ4YPwAx5TNWBcBCggACqwQ0JgcAE&#43;BGIG5gcCEWDHPfBBAX2sE6Hv1LA/qcLhTj5QxUHISiVZtAAGNsABD4AgBCLw4nXyCALicEADr1HObNIoRQJYpRQtAEECGODDCVTgAlvkQAc8MJzheMADHfijfir&#43;YKpBhk6KU2wjIYyogARgEQLxs8AFMJABDeDtla3BwAU4qaQGMGl90QuAJZqxAoIgQAENeAAqHWkBVV7gmBewQGYmIAHZLCZVuIweFb8xlJUcwJQNcAAEUjKBblKgm7HgjANsmYADFECN0SMA45pREY6Qx5QMyOYDhAkBxDxgnAygzQHygs7oAawhKthAANx5AAQkQAELWAADFprQUiJgn&#43;0A5U6oKEpEDIUCrhDAAApgloIi4KMPNUABCDAAZUgUFBA4ZFsuupMAaJQjBIgpSUvKtZN2Q6WkGcoFasqTAPiUpzYFAAVwqpuhmCOoQR1AB0SYUxVVAKhIBUUE1FI6UUkMhQQV6KdEV&#43;WkqpZiKCgoYRRBOYAHcMASTNWQIdYSKhBw4K0MBNBa1GpVtrJ1BXTNq173qohAAAA7"/>
<style>* {
font-family: inherit;
font-size: 1em;
margin: 0;
padding: 0;
}
html {
box-sizing: border-box;
font-size: 62.5%;
}
*, *:before, *:after {
box-sizing: inherit;
}
h1, h2 {
margin-bottom: 0.2em;
margin-top: 0.8em;
}
h1 {
font-size: 1.4em;
}
h2 {
font-size: 1.2em;
}
body {
font-size: 1.6em;
margin: 2px;
}
li {
margin-left: 2.5em;
}
a {
color: inherit;
text-decoration: inherit;
}
ol, ul {
margin-bottom: 0.5em;
margin-top: 0.5em;
}
p {
margin-bottom: 2em;
}
table {
margin: 0.6em;
}
table tr:nth-child(odd) {
background-color: #F0F0F0;
}
table tr:hover {
background-color: #DDD !important;
}
table td {
font-family: monospace;
padding: 0.2em 0.4em 0.2em;
}
.call {
font-family: monospace;
}
@media screen and (max-width: 500px) {
h1 {
font-size: 1.3em;
}
}
@media screen and (max-width: 500px) and (orientation: portrait) {
.args span {
display: none;
}
.args::after {
content: '…';
}
}
.created {
white-space: nowrap;
}
.race {
font-weight: 700;
color: #600;
}
#content {
width: 100%;
}
.hastooltip:hover .tooltip {
background: #fffAF0;
border: 1px solid #DCA;
border-radius: 6px;
box-shadow: 5px 5px 8px #CCC;
color: #111;
display: inline;
position: absolute;
}
.tooltip {
display: none;
line-height: 16px;
margin-left: 1rem;
margin-top: 2.5rem;
padding: 1rem;
z-index: 10;
}
.bottom-padding {
margin-top: 5em;
}.FuncMain {
color: #880;
}
.FuncLocationUnknown {
color: #888;
}
.FuncGoMod {
color: #800;
}
.FuncGOPATH {
color: #109090;
}
.FuncGoPkg {
color: #008;
}
.FuncStdlib {
color: #080;
}
.Exported {
font-weight: 700;
}
</style>
<div id="content">
<h1>Signature #0: 1 routine: <span class="state">running</span></h1>
<table class="stack"><tr>
<td>0</td>
<td>
<a href="https://godoc.org/runtime/debug#Stack">debug</a>
</td>
<td class="hastooltip">
<span class="tooltip">SrcPath: /usr/local/go/src/runtime/debug/stack.go<br>Func: runtime/debug.Stack
<br>Location: LocationUnknown
</span>
<a href="file:////usr/local/go/src/runtime/debug/stack.go">stack.go:24</a>
</td>
<td>
<span class="FuncLocationUnknown Exported"><a href="https://godoc.org/runtime/debug#Stack">Stack</a></span>(<span class="args"><span>0x434070, 0xddb11, 0, 0x40e0f8</span></span>)
</td>
</tr><tr>
<td>1</td>
<td>
<a href="https://godoc.org/runtime/debug#PrintStack">debug</a>
</td>
<td class="hastooltip">
<span class="tooltip">SrcPath: /usr/local/go/src/runtime/debug/stack.go<br>Func: runtime/debug.PrintStack
<br>Location: LocationUnknown
</span>
<a href="file:////usr/local/go/src/runtime/debug/stack.go">stack.go:16</a>
</td>
<td>
<span class="FuncLocationUnknown Exported"><a href="https://godoc.org/runtime/debug#PrintStack">PrintStack</a></span>(<span class="args"><span></span></span>)
</td>
</tr><tr>
<td>2</td>
<td>
<a href="https://godoc.org/main#main">main</a>
</td>
<td class="hastooltip">
<span class="tooltip">SrcPath: /tmp/sandbox157492124/main.go<br>Func: main.main
<br>Location: LocationUnknown
</span>
<a href="file:////tmp/sandbox157492124/main.go">main.go:6</a>
</td>
<td>
<span class="FuncMain Exported"><a href="https://godoc.org/main#main">main</a></span>(<span class="args"><span></span></span>)
</td>
</tr></table></div>
<h2>Legend</h2>
<table class="legend">
<thead>
<th>Type</th>
<th>Exported</th>
<th>Private</th>
</thead>
<tr class="call hastooltip">
<td>
Package main
<span class="tooltip">Sources that are in the main package.</span>
</td>
<td class="FuncMain">main.Foo()</td>
<td class="FuncMain">main.foo()</td>
</tr>
<tr class="call hastooltip">
<td>
Go module
<span class="tooltip">Sources located inside a directory containing a
<strong>go.mod</strong> file but outside $GOPATH.</span>
</td>
<td class="FuncGoMod Exported">pkg.Foo()</td>
<td class="FuncGoMod">pkg.foo()</td>
</tr>
<tr class="call hastooltip">
<td>
$GOPATH/src/...
<span class="tooltip">Sources located inside the traditional $GOPATH/src
directory.</span>
</td>
<td class="FuncGOPATH Exported">pkg.Foo()</td>
<td class="FuncGOPATH">pkg.foo()</td>
</tr>
<tr class="call hastooltip">
<td>
$GOPATH/pkg/mod/...
<span class="tooltip">Sources located inside the go module dependency
cache under $GOPATH/pkg/mod. These files are unmodified third parties.</span>
</td>
<td class="FuncGoPkg Exported">pkg.Foo()</td>
<td class="FuncGoPkg">pkg.foo()</td>
</tr>
<tr class="call hastooltip">
<td>
Standard library
<span class="tooltip">Sources from the Go standard library under
$GOROOT/src/.</span>
</td>
<td class="FuncStdlib Exported">pkg.Foo()</td>
<td class="FuncStdlib">pkg.foo()</td>
</tr>
<tr class="call hastooltip">
<td>
Unknown source location
<span class="tooltip">Sources which location was not successfully
determined.</span>
</td>
<td class="FuncLocationUnknown Exported">pkg.Foo()</td>
<td class="FuncLocationUnknown">pkg.foo()</td>
</tr>
</table><div class="bottom-padding"></div>
`

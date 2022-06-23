<!DOCTYPE html>

{{- /* Join a list */ -}}
{{- define "Join" -}}
  {{- if . -}}
    {{- $l := len . -}}
    {{- $last := minus $l 1 -}}
    {{- range $i, $e := . -}}
      {{- $e -}}
      {{- $isNotLast := ne $i $last -}}
      {{- if $isNotLast}}, {{end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{- /* Accepts a Args */ -}}
{{- define "RenderArgs" -}}
  <span class="args"><span>
  {{- $elided := .Elided -}}
  {{- if .Processed -}}
    {{- $l := len .Processed -}}
    {{- $last := minus $l 1 -}}
    {{- range $i, $e := .Processed -}}
      {{- $e -}}
      {{- $isNotLast := ne $i $last -}}
      {{- if or $elided $isNotLast}}, {{end -}}
    {{- end -}}
  {{- else -}}
    {{- $l := len .Values -}}
    {{- $last := minus $l 1 -}}
    {{- range $i, $e := .Values -}}
      {{- $e.String -}}
      {{- $isNotLast := ne $i $last -}}
      {{- if or $elided $isNotLast}}, {{end -}}
    {{- end -}}
  {{- end -}}
  {{- if $elided}}…{{end -}}
  </span></span>
{{- end -}}

{{- /* Accepts a Call */ -}}
{{- define "RenderCreatedBy" -}}
  <span class="call hastooltip"><span class="tooltip">
    {{- if and .LocalSrcPath (ne .RemoteSrcPath .LocalSrcPath) -}}
    RemoteSrcPath: {{.RemoteSrcPath}}
    <br>LocalSrcPath: {{.LocalSrcPath}}
    {{- else -}}
    SrcPath: {{.RemoteSrcPath}}
    {{- end -}}
    <br>Func: {{.Func.Complete}}
    <br>Location: {{.Location}}
    </span><a href="{{srcURL .}}">{{.SrcName}}:{{.Line}}</a> <span class="{{funcClass .}}">
    <a href="{{pkgURL .}}">{{.Func.DirName}}.{{.Func.Name}}</a></span>()
  </span>
{{- end -}}

{{- /* Accepts a Stack */ -}}
{{- define "RenderCalls" -}}
  <table class="stack">
    {{- range $i, $e := .Calls -}}
      <tr>
        <td>{{$i}}</td>
        <td>
          <a href="{{pkgURL $e}}">{{$e.Func.DirName}}</a>
        </td>
        <td class="hastooltip">
          <span class="tooltip">
            {{- if and $e.LocalSrcPath (ne $e.RemoteSrcPath $e.LocalSrcPath) -}}
            RemoteSrcPath: {{$e.RemoteSrcPath}}
            <br>LocalSrcPath: {{$e.LocalSrcPath}}
            {{- else -}}
            SrcPath: {{$e.RemoteSrcPath}}
            {{- end -}}
            <br>Func: {{$e.Func.Complete}}
            <br>Location: {{$e.Location}}
          </span>
          <a href="{{srcURL $e}}">{{$e.SrcName}}:{{$e.Line}}</a>
        </td>
        <td>
          <span class="{{funcClass $e}}"><a href="{{pkgURL $e}}">{{$e.Func.Name}}</a></span>({{template "RenderArgs" $e.Args}})
        </td>
      </tr>
    {{- end -}}
    {{- if .Elided}}<tr><td>(…)</td><tr>{{end -}}
  </table>
{{- end -}}

<meta charset="UTF-8">
<meta name="author" content="Marc-Antoine Ruel" >
<meta name="generator" content="https://github.com/maruel/panicparse" >
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PanicParse</title>
<link rel="shortcut icon" type="image/gif" href="data:image/gif;base64,{{.Favicon}}"/>
<style>
  {{- /* Minimal CSS reset */ -}}
  * {
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
  }

  {{- /* Highlights based on stack.Location value. */ -}}
  .FuncMain {
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
  {{- if .Aggregated -}}
    {{- range $i, $e := .Aggregated.Buckets -}}
      {{$l := len $e.IDs}}
      <h1>Signature #{{$i}}: {{$l}} routine{{if ne 1 $l}}s{{end}}: <span class="state">{{$e.State}}</span>
      {{- if $e.SleepMax -}}
        {{- if ne $e.SleepMin $e.SleepMax}} <span class="sleep">[{{$e.SleepMin}}~{{$e.SleepMax}} mins]</span>
        {{- else}} <span class="sleep">[{{$e.SleepMax}} mins]</span>
        {{- end -}}
      {{- end -}}
      </h1>
      {{if $e.Locked}} <span class="locked">[locked]</span>
      {{- end -}}
      {{- if $e.CreatedBy.Calls}} <span class="created">Created by: {{template "RenderCreatedBy" index $e.CreatedBy.Calls 0}}</span>
      {{- end -}}
      {{template "RenderCalls" $e.Signature.Stack}}
    {{- end -}}
  {{- else -}}
    {{- range $i, $e := .Snapshot.Goroutines -}}
      <h1>Routine {{$e.ID}}: <span class="state">{{$e.State}}</span>
      {{- if $e.SleepMax -}}
        {{- if ne $e.SleepMin $e.SleepMax}} <span class="sleep">[{{$e.SleepMin}}~{{$e.SleepMax}} mins]</span>
        {{- else}} <span class="sleep">[{{$e.SleepMax}} mins]</span>
        {{- end -}}
      {{- end -}}
      </h1>
      {{if $e.Locked}} <span class="locked">[locked]</span>
      {{- end -}}
      {{if $e.RaceAddr}} <span class="race">Race {{if $e.RaceWrite}}write{{else}}read{{end}} @ {{printf "0x%08X" $e.RaceAddr}}</span><br>
      {{- end -}}
      {{- if $e.CreatedBy.Calls}} <span class="created">Created by: {{template "RenderCreatedBy" index $e.CreatedBy.Calls 0}}</span>
      {{- end -}}
      {{template "RenderCalls" $e.Signature.Stack}}
    {{- end -}}
  {{- end -}}
</div>
<h2>Metadata</h2>
<ul>
  <li>Created on {{.Now.String}}</li>
  <li>{{.Version}}</li>
  {{- if and .Snapshot.LocalGOROOT (ne .Snapshot.RemoteGOROOT .Snapshot.LocalGOROOT) -}}
    <li>GOROOT (remote): {{.Snapshot.RemoteGOROOT}}</li>
    <li>GOROOT (local): {{.Snapshot.LocalGOROOT}}</li>
  {{- else -}}
    <li>GOROOT: {{.Snapshot.RemoteGOROOT}}</li>
  {{- end -}}
  <li>GOPATH: {{template "Join" .Snapshot.LocalGOPATHs}}</li>
  {{- if .Snapshot.LocalGomods -}}
    <li>go modules (local):
      <ul>
      {{- range $path, $import := .Snapshot.LocalGomods -}}
        <li>{{$path}}: {{$import}}</li>
      {{- end -}}
      </ul>
    </li>
  {{- end -}}
  <li>GOMAXPROCS: {{.GOMAXPROCS}}</li>
</ul>
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
</table>
{{- .Footer -}}
{{- /* Add unnecessary bottom spacing so the last tooltip from the legend is visible. */ -}}
<div class="bottom-padding"></div>

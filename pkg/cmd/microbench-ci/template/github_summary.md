{{- range .GitHubSummaryData}}
<details><summary><strong>{{.BenchmarkStatus}} {{.DisplayName}}</strong> [{{.Labels}}]</summary>

| Metric                      | Old Commit     | New Commit     | Delta      | Note         |
|-----------------------------|----------------|----------------|------------|--------------|
{{- range .Summaries}}
| {{.Status}} **{{.Metric}}** | {{.OldCenter}} | {{.NewCenter}} | {{.Delta}} | {{.Note}}    |
{{- end}}

<details><summary>Reproduce</summary>

**benchdiff binaries**:
```shell
{{- $benchdiff := .Benchdiff}}
{{- range $rev, $dir := $benchdiff.Dir }}
mkdir -p {{$dir}}
gcloud storage cp {{index $benchdiff.BinURL $rev}} {{index $benchdiff.BinDest $rev}}
chmod +x {{index $benchdiff.BinDest $rev}}
{{- end}}
```
**benchdiff command**:
```shell
# NB: for best (most stable) results, also add a suitable `--benchtime` that
# results in ~1s to ~5s of benchmark runs. For example, if ops average ~3ms, a
# benchtime of `1000x` is appropriate.
#
# Some benchmarks (in particular BenchmarkSysbench) output additional memory
# profiles covering only the execution (excluding the setup/teardown) - those
# should be preferred for analysis since they more closely correspond to what's
# reported as B/op and alloc/op.
benchdiff --run=^{{$benchdiff.Run}}$ --old={{index $benchdiff.TrimmedSHA $benchdiff.Old}} --new={{index $benchdiff.TrimmedSHA $benchdiff.New}} --memprofile ./{{$benchdiff.Package}}
```

</details>

</details>
{{- end}}

<details><summary>Artifacts</summary>

**download**:
```shell
{{- range $rev, $url := $.Artifacts }}
mkdir -p {{$rev}}
gcloud storage cp {{$url}}\* {{$rev}}/
{{- end}}
```

</details>

_built with commit: [{{.Commit}}](https://github.com/cockroachdb/cockroach/commit/{{.Commit}})_

{{- range .GitHubSummaryData}}
<details><summary><strong>{{.BenchmarkStatus}} {{.DisplayName}}</strong> [{{.Labels}}]</summary>

| Metric                      | Old Commit     | New Commit     | Delta      | Note         | Threshold      |
|-----------------------------|----------------|----------------|------------|--------------|----------------|
{{- range .Summaries}}
| {{.Status}} **{{.Metric}}** | {{.OldCenter}} | {{.NewCenter}} | {{.Delta}} | {{.Note}}    | {{.Threshold}} |
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
benchdiff --run=^{{$benchdiff.Run}}$ --old={{index $benchdiff.TrimmedSHA $benchdiff.Old}} --new={{index $benchdiff.TrimmedSHA $benchdiff.New}} ./{{$benchdiff.Package}}
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

<details><summary>Legend</summary>

- ⚪ **Neutral:** No significant performance change.
- 🟡 **Warning:** Slight degradation, likely due to variance, but still within thresholds.
- 🔴 **Regression:** Likely performance regression, requiring investigation.
- 🟢 **Improvement:** Possible performance gain.

</details>

{{.Description}}

_built with commit: [{{.Commit}}](https://github.com/cockroachdb/cockroach/commit/{{.Commit}})_

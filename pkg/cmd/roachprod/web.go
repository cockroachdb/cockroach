// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

func web(dirs []string) error {
	// TODO(peter): visualize the output of a single test run, showing
	// performance and latency over time.
	switch n := len(dirs); n {
	case 0:
		return fmt.Errorf("no test directory specified")
	case 1, 2:
		d1, err := loadTestData(dirs[0])
		if err != nil {
			return err
		}
		if n == 1 {
			return web1(d1)
		}
		d2, err := loadTestData(dirs[1])
		if err != nil {
			return err
		}
		return web2(d1, d2)
	default:
		ds := make([]*testData, len(dirs))
		for i, dir := range dirs {
			d, err := loadTestData(dir)
			if err != nil {
				return err
			}

			ds[i] = d
		}
		return webBulk(ds)
	}
}

func webApply(m interface{}) error {
	t, err := template.New("web").Parse(webHTML)
	if err != nil {
		return err
	}
	dir, err := ioutil.TempDir("", "roachprod-web")
	if err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "index.html")) // .html extension required for open to work
	if err != nil {
		return err
	}
	defer f.Close()
	if err := t.Execute(f, m); err != nil {
		return err
	}
	return exec.Command("open", f.Name()).Run()
}

type series struct {
	TargetAxisIndex int
	Color           string
	LineDashStyle   []int
}

func web1(d *testData) error {
	data := []interface{}{
		[]interface{}{"concurrency", "ops/sec", "avg latency", "99%-tile latency"},
	}
	for _, r := range d.Runs {
		data = append(data, []interface{}{
			r.Concurrency, r.OpsSec, r.AvgLat, r.P99Lat,
		})
	}

	m := map[string]interface{}{
		"data":  data,
		"haxis": "concurrency",
		"vaxes": []string{"ops/sec", "latency (ms)"},
		"series": []series{
			{0, "#ff0000", []int{}},
			{1, "#ff0000", []int{2, 2}},
			{1, "#ff0000", []int{4, 4}},
		},
	}

	return webApply(m)
}

func web2(d1, d2 *testData) error {
	d1, d2 = alignTestData(d1, d2)

	data := []interface{}{
		[]interface{}{
			"concurrency",
			fmt.Sprintf("ops/sec (%s)", d1.Metadata.Bin),
			fmt.Sprintf("99%%-lat (%s)", d1.Metadata.Bin),
			fmt.Sprintf("ops/sec (%s)", d2.Metadata.Bin),
			fmt.Sprintf("99%%-lat (%s)", d2.Metadata.Bin),
		},
	}
	for i := range d1.Runs {
		r1 := d1.Runs[i]
		r2 := d2.Runs[i]
		data = append(data, []interface{}{
			r1.Concurrency, r1.OpsSec, r1.P99Lat, r2.OpsSec, r2.P99Lat,
		})
	}

	m := map[string]interface{}{
		"data":  data,
		"haxis": "concurrency",
		"vaxes": []string{"ops/sec", "latency (ms)"},
		"series": []series{
			{0, "#ff0000", []int{}},
			{1, "#ff0000", []int{2, 2}},
			{0, "#0000ff", []int{}},
			{1, "#0000ff", []int{2, 2}},
		},
	}
	return webApply(m)
}

const webHTML = `<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['corechart']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          {{- range .data }}
          {{ . }},
          {{- end}}
        ]);

        var options = {
          legend: { position: 'top', alignment: 'center', textStyle: {fontSize: 12}, maxLines: 5 },
          crosshair: { trigger: 'both', opacity: 0.35 },
          series: {
            {{- range $i, $e := .series }}
            {{ $i }}: {targetAxisIndex: {{- $e.TargetAxisIndex }}, color: {{ $e.Color }}, lineDashStyle: {{ $e.LineDashStyle }}},
            {{- end }}
          },
          vAxes: {
            {{- range $i, $e := .vaxes }}
            {{ $i }}: {title: {{ $e }}},
            {{- end }}
          },
          hAxis: {
            title: {{ .haxis }},
          },
        };
        var chart = new google.visualization.LineChart(document.getElementById('chart'));
        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <div id="chart" style="width: 800; height: 600"></div>
  </body>
</html>
`

func webBulk(m []*testData) error {
	t, err := template.New("web").Parse(webHTMLBulk)
	if err != nil {
		return err
	}
	f, err := ioutil.TempFile("", "web")
	if err != nil {
		return err
	}
	defer f.Close()
	if err := t.Execute(f, m); err != nil {
		return err
	}
	return exec.Command("open", f.Name()).Run()
}

const webHTMLBulk = `<html>
  <head>
  </head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script>
      google.charts.load('current', {'packages':['corechart']});
      google.charts.setOnLoadCallback(renderOverview);

      var SUMMARY_CONCURRENCY = 384;

      var tests = [
      {{- range $j, $d := . }}{{ if $j }},{{ end }}
        {
          "metadata": {
            "bin": {{ .Metadata.Bin }},
            "cluster": {{ .Metadata.Cluster }},
            "nodes": {{ .Metadata.Nodes }},
            "env": {{ .Metadata.Env }},
            "args": {{ .Metadata.Args }},
            "test": {{ .Metadata.Test }},
            "date": {{ .Metadata.Date }}
          },
          "runs": [
          {{- range $i, $e := .Runs }}{{ if $i }},{{ end }}
            {
              "concurrency": {{ $e.Concurrency }},
              "elapsed": {{ $e.Elapsed }},
              "errors": {{ $e.Errors }},
              "ops": {{ $e.Ops }},
              "opsSec": {{ $e.OpsSec }},
              "avgLat": {{ $e.AvgLat }},
              "p50Lat": {{ $e.P50Lat }},
              "p95Lat": {{ $e.P95Lat }},
              "p99Lat": {{ $e.P99Lat }}
            }
          {{- end }}
          ]
        }
      {{- end }}
      ];

      var summaryOptions = {
        legend: { position: 'top', alignment: 'center', textStyle: {fontSize: 12}, maxLines: 5 },
        crosshair: { trigger: 'both', opacity: 0.35 },
        series: {
          "0":{targetAxisIndex: 0, color:"#ff0000", lineDashStyle: []},
          "1":{targetAxisIndex: 1, color:"#ff0000", lineDashStyle: [2, 2]},
          "2":{targetAxisIndex: 1, color:"#ff0000", lineDashStyle: [4, 4]}
        },
        vAxes: {"0":{title:"ops/sec"}, "1":{title:"latency (ms)"}},
        hAxis: {
          title: "version",
        },
      };

      var oneTestOptions = {
        legend: { position: 'top', alignment: 'center', textStyle: {fontSize: 12}, maxLines: 5 },
        crosshair: { trigger: 'both', opacity: 0.35 },
        series: {
          "0":{targetAxisIndex: 0, color:"#ff0000", lineDashStyle: []},
          "1":{targetAxisIndex: 1, color:"#ff0000", lineDashStyle: [2, 2]},
          "2":{targetAxisIndex: 1, color:"#ff0000", lineDashStyle: [4, 4]}
        },
        vAxes: {"0":{title:"ops/sec"}, "1":{title:"latency (ms)"}},
        hAxis: {
          title: "concurrency",
        },
      };

      var twoTestOptions = {
        legend: { position: 'top', alignment: 'center', textStyle: {fontSize: 12}, maxLines: 5 },
        crosshair: { trigger: 'both', opacity: 0.35 },
        series: {
          "0":{targetAxisIndex: 0, color:"#ff0000", lineDashStyle: []},
          "1":{targetAxisIndex: 1, color:"#ff0000", lineDashStyle: [2, 2]},
          "2":{targetAxisIndex: 0, color:"#0000ff", lineDashStyle: []},
          "3":{targetAxisIndex: 1, color:"#0000ff", lineDashStyle: [2, 2]}
        },
        vAxes: {"0":{title:"ops/sec"}, "1":{title:"latency (ms)"}},
        hAxis: {
          title: "concurrency",
        },
      };

      var compare = [];

      function renderOverview() {
        var summary = tests
          .map(function (t) {
            return {
              bin: t.metadata.bin,
              runs: t.runs.filter(function (r) { return r.concurrency === SUMMARY_CONCURRENCY; })
            };
          })
          .filter(function (t) { return t.runs.length; })
          .map(function (t) { return { bin: t.bin, run: t.runs[0] }; });

        var test;

        var source = [["version", "ops/sec", "avg latency", "99%-ile latency"]];
        for (var i = 0; i < summary.length; i++) {
          test = summary[i];
          source.push([test.bin, test.run.opsSec, test.run.avgLat, test.run.p99Lat]);
        }

        var data = google.visualization.arrayToDataTable(source);
        var chart = new google.visualization.LineChart(document.getElementById('chart'));
        chart.draw(data, summaryOptions);

        document.getElementById("label").innerHTML = 'overview';
      }

      function renderChart(i) {
        var runs = tests[i].runs;
		var bin = tests[i].metadata.bin;
        var run;

        var source = [["concurrency", "ops/sec", "avg latency", "99%-ile latency"]];
        for (var i = 0; i < runs.length; i++) {
          run = runs[i];
          source.push([run.concurrency, run.opsSec, run.avgLat, run.p99Lat]);
        }

        var data = google.visualization.arrayToDataTable(source);
        var chart = new google.visualization.LineChart(document.getElementById('chart'));
        chart.draw(data, oneTestOptions);

        document.getElementById("label").innerHTML = bin;
      }

      function compareChart(i) {
        compare.push(i);

        if (compare.length > 2) compare = compare.slice(compare.length - 2);

        if (compare.length !== 2) return;

        var d1 = tests[compare[0]], d2 = tests[compare[1]];
        var bin1 = d1.metadata.bin, bin2 = d2.metadata.bin;

        var byConcurrency1 = {}, byConcurrency2 = {};

        var concurrencies1 = d1.runs.map(function (r) {
          byConcurrency1[r.concurrency] = r;
          return r.concurrency;
        });
        var concurrencies2 = d2.runs.map(function (r) {
          byConcurrency2[r.concurrency] = r;
          return r.concurrency;
        });

        var concurrencies = concurrencies1.filter(function (c) { return concurrencies2.indexOf(c) >= 0; });

        var concurrency, run1, run2;

        var source = [["concurrency", "ops/sec (" + bin1 + ")", "99%-ile (" + bin1 + ")", "ops/sec (" + bin2 + ")", "99%-ile (" + bin2 + ")"]];
        for (var i = 0; i < concurrencies.length; i++) {
          concurrency = concurrencies[i];
          run1 = byConcurrency1[concurrency];
          run2 = byConcurrency2[concurrency];

          source.push([concurrency, run1.opsSec, run1.p99Lat, run2.opsSec, run2.p99Lat]);
        }

        var data = google.visualization.arrayToDataTable(source);
        var chart = new google.visualization.LineChart(document.getElementById('chart'));
        chart.draw(data, twoTestOptions);

        document.getElementById("label").innerHTML = bin1 + ' vs. ' + bin2;
      }
    </script>
  <body>
    <h2>performance review</h2>
	<p>Chart a single test by clicking "single".  Compare two by clicking "compare" on each.</p>
    <h3>available tests</h3>
    <ul>
      <li><a href="#" onClick="renderOverview()">overview</a></li>
    {{- range $i, $e := . }}
      <li>
        {{ .Metadata.Bin }}
        {{- if .Metadata.Date }} ({{ .Metadata.Date }}){{ end }}
        - <a href="#" onClick="renderChart({{ $i }});">single</a>
        - <a href="#" onClick="compareChart({{ $i }});">compare</a>
      </li>
    {{- end }}
    </ul>
	<h3 id="label"></h3>
    <div id="chart" style="width: 800; height: 600"></div>
  </body>
</html>
`

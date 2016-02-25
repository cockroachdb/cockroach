package main

import (
	"fmt"
	"io"
	"strconv"
	"text/template"
	"time"
)

const (
	xmlDeclaration = `<?xml version="1.0" encoding="utf-8"?>`

	xunitTemplate string = `
{{range $suite := .Suites}}  <testsuite name="{{.Name}}" tests="{{.Count}}" errors="0" failures="{{.NumFailed}}" skip="{{.NumSkipped}}">
{{range  $test := $suite.Tests}}    <testcase classname="{{$suite.Name}}" name="{{$test.Name}}" time="{{$test.Time}}">
{{if $test.Skipped }}      <skipped/> {{end}}
{{if $test.Failed }}      <failure type="go.error" message="error">
        <![CDATA[{{$test.Message}}]]>
      </failure>{{end}}    </testcase>
{{end}}  </testsuite>
{{end}}`

	multiTemplate string = `
<testsuites>` + xunitTemplate + `</testsuites>
`

	// https://xunit.codeplex.com/wikipage?title=XmlFormat
	xunitNetTemplate string = `
<assembly name="{{.Assembly}}"
          run-date="{{.RunDate}}" run-time="{{.RunTime}}"
          configFile="none"
          time="{{.Time}}"
          total="{{.Total}}"
          passed="{{.Passed}}"
          failed="{{.Failed}}"
          skipped="{{.Skipped}}"
          environment="n/a"
          test-framework="golang">
{{range $suite := .Suites}}
    <class time="{{.Time}}" name="{{.Name}}"
  	     total="{{.Count}}"
  	     passed="{{.NumPassed}}"
  	     failed="{{.NumFailed}}"
  	     skipped="{{.NumSkipped}}">
{{range  $test := $suite.Tests}}
        <test name="{{$test.Name}}"
          type="test"
          method="{{$test.Name}}"
          result={{if $test.Skipped }}"Skip"{{else if $test.Failed }}"Fail"{{else if $test.Passed }}"Pass"{{end}}
          time="{{$test.Time}}">
        {{if $test.Failed }}  <failure exception-type="go.error">
             <message><![CDATA[{{$test.Message}}]]></message>
      	  </failure>
      	{{end}}</test>
{{end}}
    </class>
{{end}}
</assembly>
`
)

// TestResults is passed to XML template
type TestResults struct {
	Suites   []*Suite
	Assembly string
	RunDate  string
	RunTime  string
	Time     string
	Total    int
	Passed   int
	Failed   int
	Skipped  int
}

// calcTotals calculates grand total for all suites
func (r *TestResults) calcTotals() {
	totalTime, _ := strconv.ParseFloat(r.Time, 64)
	for _, suite := range r.Suites {
		r.Passed += suite.NumPassed()
		r.Failed += suite.NumFailed()
		r.Skipped += suite.NumSkipped()

		suiteTime, _ := strconv.ParseFloat(suite.Time, 64)
		totalTime += suiteTime
		r.Time = fmt.Sprintf("%.3f", totalTime)
	}
	r.Total = r.Passed + r.Skipped + r.Failed
}

// writeXML exits xunit XML of tests to out
func writeXML(suites []*Suite, out io.Writer, xmlTemplate string, testTime time.Time) {
	testsResult := TestResults{
		Suites:   suites,
		Assembly: suites[len(suites)-1].Name,
		RunDate:  testTime.Format("2006-01-02"),
		RunTime:  testTime.Format("15:04:05"),
	}
	testsResult.calcTotals()
	t := template.New("test template")

	t, err := t.Parse(xmlDeclaration + xmlTemplate)
	if err != nil {
		fmt.Printf("Error in parse %v\n", err)
		return
	}
	err = t.Execute(out, testsResult)
	if err != nil {
		fmt.Printf("Error in execute %v\n", err)
		return
	}
}

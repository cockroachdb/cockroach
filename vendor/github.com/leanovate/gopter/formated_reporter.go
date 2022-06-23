package gopter

import (
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
)

const newLine = "\n"

// FormatedReporter reports test results in a human readable manager.
type FormatedReporter struct {
	verbose bool
	width   int
	output  io.Writer
}

// NewFormatedReporter create a new formated reporter
// verbose toggles verbose output of the property results
// width is the maximal width per line
// output is the writer were the report will be written to
func NewFormatedReporter(verbose bool, width int, output io.Writer) Reporter {
	return &FormatedReporter{
		verbose: verbose,
		width:   width,
		output:  output,
	}
}

// ConsoleReporter creates a FormatedReporter writing to the console (i.e. stdout)
func ConsoleReporter(verbose bool) Reporter {
	return NewFormatedReporter(verbose, 75, os.Stdout)
}

// ReportTestResult reports a single property result
func (r *FormatedReporter) ReportTestResult(propName string, result *TestResult) {
	if result.Passed() {
		fmt.Fprintln(r.output, r.formatLines(fmt.Sprintf("+ %s: %s", propName, r.reportResult(result)), "", ""))
	} else {
		fmt.Fprintln(r.output, r.formatLines(fmt.Sprintf("! %s: %s", propName, r.reportResult(result)), "", ""))
	}
}

func (r *FormatedReporter) reportResult(result *TestResult) string {
	status := ""
	switch result.Status {
	case TestProved:
		status = "OK, proved property.\n" + r.reportPropArgs(result.Args)
	case TestPassed:
		status = fmt.Sprintf("OK, passed %d tests.", result.Succeeded)
	case TestFailed:
		status = fmt.Sprintf("Falsified after %d passed tests.\n%s%s", result.Succeeded, r.reportLabels(result.Labels), r.reportPropArgs(result.Args))
	case TestExhausted:
		status = fmt.Sprintf("Gave up after only %d passed tests. %d tests were discarded.", result.Succeeded, result.Discarded)
	case TestError:
		if r.verbose {
			status = fmt.Sprintf("Error on property evaluation after %d passed tests: %s\n%s\n%s", result.Succeeded, result.Error.Error(), result.ErrorStack, r.reportPropArgs(result.Args))
		} else {
			status = fmt.Sprintf("Error on property evaluation after %d passed tests: %s\n%s", result.Succeeded, result.Error.Error(), r.reportPropArgs(result.Args))
		}
	}

	if r.verbose {
		return concatLines(status, fmt.Sprintf("Elapsed time: %s", result.Time.String()))
	}
	return status
}

func (r *FormatedReporter) reportLabels(labels []string) string {
	if labels != nil && len(labels) > 0 {
		return fmt.Sprintf("> Labels of failing property: %s\n", strings.Join(labels, newLine))
	}
	return ""
}

func (r *FormatedReporter) reportPropArgs(p PropArgs) string {
	result := ""
	for i, arg := range p {
		if result != "" {
			result += newLine
		}
		result += r.reportPropArg(i, arg)
	}
	return result
}

func (r *FormatedReporter) reportPropArg(idx int, propArg *PropArg) string {
	label := propArg.Label
	if label == "" {
		label = fmt.Sprintf("ARG_%d", idx)
	}
	result := fmt.Sprintf("%s: %v", label, propArg.Arg)
	if propArg.Shrinks > 0 {
		result += fmt.Sprintf("\n%s_ORIGINAL (%d shrinks): %v", label, propArg.Shrinks, propArg.OrigArg)
	}

	return result
}

func (r *FormatedReporter) formatLines(str, lead, trail string) string {
	result := ""
	for _, line := range strings.Split(str, "\n") {
		if result != "" {
			result += newLine
		}
		result += r.breakLine(lead+line+trail, "  ")
	}
	return result
}

func (r *FormatedReporter) breakLine(str, lead string) string {
	if len(str) <= r.width {
		return str
	}

	result := ""
	for len(str) > r.width {
		idx := strings.LastIndexFunc(str[0:r.width], func(ch rune) bool {
			return unicode.IsSpace(ch)
		})
		if idx <= 0 {
			idx = r.width
		}
		result += str[0:idx] + "\n" + lead
		str = str[idx:]
	}
	result += str
	return result
}

func concatLines(strs ...string) string {
	result := ""
	for _, str := range strs {
		if str != "" {
			if result != "" {
				result += "\n"
			}
			result += str
		}
	}
	return result
}

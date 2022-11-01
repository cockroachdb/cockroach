package main

import (
	"strings"
)

func extractBenchmarkResults(benchmarkOutput string) [][]string {
	results := make([][]string, 0)
	buf := make([]string, 0)
	var benchName string
	for _, line := range strings.Split(benchmarkOutput, "\n") {
		elems := strings.Fields(line)
		for index, s := range elems {
			if strings.HasPrefix(s, "Benchmark") && len(s) > 9 {
				benchName = s
			}
			if s == "ns/op" {
				er := elems[index-2:]
				buf = append(buf, er...)
				if benchName != "" {
					buf = append([]string{benchName}, buf...)
					results = append(results, buf)
				}
				buf = make([]string, 0)
				benchName = ""
			}
		}
	}
	return results
}

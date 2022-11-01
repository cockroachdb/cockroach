package main

import (
	"fmt"
	"os"
	"path/filepath"
)

var (
	benchmarkOutput *os.File
	analyticsOutput *os.File
)

func openReports() error {
	var err error
	benchmarkOutput, err = os.Create(filepath.Join(benchDir,
		fmt.Sprintf("report-%s.log", timestamp.Format(timeFormat))))
	if err != nil {
		return err
	}
	analyticsOutput, err = os.Create(filepath.Join(benchDir,
		fmt.Sprintf("analytics-%s.log", timestamp.Format(timeFormat))))
	if err != nil {
		return err
	}
	return nil
}

func closeReports() {
	if benchmarkOutput != nil {
		err := benchmarkOutput.Close()
		if err != nil {
			l.Errorf("Error closing benchmark output file: %s", err)
		}
	}
	if analyticsOutput != nil {
		err := analyticsOutput.Close()
		if err != nil {
			l.Errorf("Error closing analytics output file: %s", err)
		}
	}
}

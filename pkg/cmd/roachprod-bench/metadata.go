package main

import (
	"bufio"
	"fmt"
	"os"
)

func readManifest(benchDir string) ([]string, error) {
	file, err := os.Open(fmt.Sprintf("%s/roachbench.manifest", benchDir))
	if err != nil {
		l.Errorf("Failed to open manifest file - %v", err)
		return nil, err
	}

	defer func(file *os.File) {
		if defErr := file.Close(); defErr != nil {
			l.Errorf("Failed to close manifest file - %v", err)
		}
	}(file)

	entries := make([]string, 0)
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		entries = append(entries, sc.Text())
	}

	if err = sc.Err(); err != nil {
		l.Errorf("Failed to read manifest file - %v", err)
		return nil, err
	}

	return entries, nil
}

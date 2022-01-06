// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Usage: go run ./ListenFileChange.go parent_folder_path file_name [timeout_duration]

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type result struct {
	finished bool
	err      error
}

const defaultTimeout = 30

func main() {

	if len(os.Args) < 2 {
		panic(fmt.Errorf("must provide the folder to watch and the file to listen to"))
	}

	var err error

	folderPath := os.Args[1]
	wantedFileName := os.Args[2]

	timeout := defaultTimeout

	if len(os.Args) > 3 {
		timeoutArg := os.Args[3]
		timeout, err = strconv.Atoi(timeoutArg)
		if err != nil {
			panic(fmt.Errorf("timeout argument must be an integer: %v", err))
		}
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan result)

	go func() {
		for {
			if _, err := os.Stat(filepath.Join(folderPath, wantedFileName)); errors.Is(err, os.ErrNotExist) {
			} else {
				done <- result{finished: true, err: nil}
			}
			time.Sleep(time.Second * 1)
		}
	}()

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				fileName := event.Name[strings.LastIndex(event.Name, "/")+1:]
				if event.Op&fsnotify.Write == fsnotify.Write && fileName == wantedFileName {
					done <- result{finished: true, err: nil}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				done <- result{finished: false, err: err}
			}
		}
	}()

	err = watcher.Add(folderPath)
	if err != nil {
		fmt.Printf("error: %v", err)
		return
	}

	select {
	case res := <-done:
		if res.finished && res.err == nil {
			fmt.Println("finished")
		} else {
			fmt.Printf("error: %v", res.err)
		}

	case <-time.After(time.Duration(timeout) * time.Second):
		fmt.Printf("timeout for %d second", timeout)
	}

	return
}

// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package fileutil

import (
	"io"
	"os"
)

// WriteFileSyncing is esseially ioutil.WriteFile -- writes data to a file named
// by filename -- but with an fsync at `syncBytes` intervals. If the file does
// not exist, WriteFile creates it with permissions perm; otherwise WriteFile
// truncates it before writing.
func WriteFileSyncing(filename string, data []byte, perm os.FileMode, syncBytes int) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	for i := 0; i < len(data); i += syncBytes {
		end := i + syncBytes
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]

		var wrote int
		wrote, err = f.Write(chunk)
		if err == nil && wrote < len(chunk) {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = f.Sync()
		}
		if err != nil {
			break
		}
	}

	if err == nil {
		err = f.Close()
	}
	return err
}

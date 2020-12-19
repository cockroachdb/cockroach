// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bufio"
	"encoding/base64"
	gohex "encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/errors"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

func runDebugDecodeProto(_ *cobra.Command, _ []string) error {
	if isatty.IsTerminal(os.Stdin.Fd()) {
		fmt.Fprintln(stderr,
			`# Reading proto-encoded pieces of data from stdin.
# Press Ctrl+C or Ctrl+D to terminate.`,
		)
	}
	return streamMap(os.Stdout, os.Stdin,
		func(s string) (bool, string, error) {
			return tryDecodeValue(s, debugDecodeProtoName, debugDecodeProtoEmitDefaults)
		})
}

// streamMap applies `fn` to all the scanned fields in `in`, and reports
// the result of `fn` on `out`.
// Errors returned by `fn` are emitted on `out` with a "warning" prefix.
func streamMap(out io.Writer, in io.Reader, fn func(string) (bool, string, error)) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(nil, 128<<20 /* 128 MiB */)
	for sc.Scan() {
		for _, field := range strings.Fields(sc.Text()) {
			ok, value, err := fn(field)
			if err != nil {
				fmt.Fprintf(out, "warning:  %v", err)
				continue
			}
			if !ok {
				fmt.Fprintf(out, "%s\t", field)
				// Skip since it doesn't appear that this field is an encoded proto.
				continue
			}
			fmt.Fprintf(out, "%s\t", value)
		}
		fmt.Fprintln(out, "")
	}
	return sc.Err()
}

// tryDecodeValue tries to decode the given string with the given proto name
// reports ok=false if the data was not valid proto-encoded.
func tryDecodeValue(s, protoName string, emitDefaults bool) (ok bool, val string, err error) {
	bytes, err := gohex.DecodeString(s)
	if err != nil {
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return false, "", nil //nolint:returnerrcheck
		}
		bytes = b
	}
	msg, err := protoreflect.DecodeMessage(protoName, bytes)
	if err != nil {
		return false, "", nil //nolint:returnerrcheck
	}
	j, err := protoreflect.MessageToJSON(msg, emitDefaults)
	if err != nil {
		// Unexpected error: the data was valid protobuf, but does not
		// reflect back to JSON. We report the protobuf struct in the
		// error message nonetheless.
		return false, "", errors.Wrapf(err, "while JSON-encoding %#v", msg)
	}
	return true, j.String(), nil
}

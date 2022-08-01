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
	"bytes"
	"encoding/base64"
	gohex "encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

func runDebugDecodeProto(_ *cobra.Command, _ []string) error {
	if debugDecodeProtoBinaryOutput && debugDecodeProtoOutputFile == "" {
		return errors.Errorf("--out is required when --binary is specified. Redirecting stdout is not " +
			"supported because that can introduce a trailing newline character.")
	}

	if isatty.IsTerminal(os.Stdin.Fd()) {
		fmt.Fprintln(stderr,
			`# Reading proto-encoded pieces of data from stdin.
# Press Ctrl+C or Ctrl+D to terminate.`,
		)
	}
	out := os.Stdout
	if debugDecodeProtoOutputFile != "" {
		var err error
		out, err = os.OpenFile(debugDecodeProtoOutputFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}

	if debugDecodeProtoSingleProto {
		buf := bytes.NewBuffer(nil)
		_, err := buf.ReadFrom(os.Stdin)
		if err != nil {
			return err
		}
		msg := tryDecodeValue(string(buf.Bytes()), debugDecodeProtoName)
		if msg == nil {
			return errors.Errorf("decoding failed")
		}
		bytes, err := protoutil.Marshal(msg)
		if err != nil {
			return err
		}

		if debugDecodeProtoBinaryOutput {
			_, err := out.Write(bytes)
			if err != nil {
				return err
			}
		} else {
			j, err := protoreflect.MessageToJSON(msg, protoreflect.FmtFlags{EmitDefaults: debugDecodeProtoEmitDefaults})
			if err != nil {
				// Unexpected error: the data was valid protobuf, but does not
				// reflect back to JSON. We report the protobuf struct in the
				// error message nonetheless.
				return errors.Wrapf(err, "while JSON-encoding %#v", msg)
			}
			fmt.Fprint(out, j)
		}
		return nil
	}

	return streamMap(out, os.Stdin,
		func(s string) (bool, string, error) {
			msg := tryDecodeValue(s, debugDecodeProtoName)
			if msg == nil {
				return false, "", nil
			}

			j, err := protoreflect.MessageToJSON(msg, protoreflect.FmtFlags{EmitDefaults: debugDecodeProtoEmitDefaults})
			if err != nil {
				// Unexpected error: the data was valid protobuf, but does not
				// reflect back to JSON. We report the protobuf struct in the
				// error message nonetheless.
				return false, "", errors.Wrapf(err, "while JSON-encoding %#v", msg)
			}
			return true, j.String(), nil
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

// interpretString decodes s from one of a couple of supported encodings:
// - hex
// - base-64
// - Go (or C) quoted string
func interpretString(s string) ([]byte, bool) {
	// Try hex.
	bytes, err := gohex.DecodeString(s)
	if err == nil {
		return bytes, true
	}
	// Try base64.
	bytes, err = base64.StdEncoding.DecodeString(s)
	if err == nil {
		return bytes, true
	}
	// Try quoted string.
	s = strings.TrimSpace(s)
	// Remove wrapping quotes, if any.
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		s = s[1 : len(s)-1]
	}
	// Add wrapping quotes; strconv.Unquote requires them.
	s = fmt.Sprintf("\"%s\"", s)
	unquoted, err := strconv.Unquote(s)
	if err == nil {
		return []byte(unquoted), true
	}
	return nil, false
}

// tryDecodeValue tries to decode the given string with the given proto name
// reports ok=false if the data was not valid proto-encoded.
func tryDecodeValue(s, protoName string) protoutil.Message {
	bytes, ok := interpretString(s)
	if !ok {
		return nil
	}
	msg, err := protoreflect.DecodeMessage(protoName, bytes)
	if err == nil {
		return msg
	}

	bytes, ok = convertFromUTF8(bytes)
	if !ok {
		return nil
	}
	msg, err = protoreflect.DecodeMessage(protoName, bytes)
	return msg
}

func convertFromUTF8(bytes []byte) (out []byte, ok bool) {
	for len(bytes) > 0 {
		// We expect only one-byte runes, which encode to one or two UTF-8 bytes.
		// That's sufficient for how (I think) Chrome treats the raw bytes that it
		// encodes to UTF-8.
		got, n := utf8.DecodeRune(bytes)
		if got > 0xff || n > 2 {
			// Unexpected multi-byte rune.
			return nil, false
		}
		out = append(out, byte(got))
		bytes = bytes[n:]
	}
	return out, true
}

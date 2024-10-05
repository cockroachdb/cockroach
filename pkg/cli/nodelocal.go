// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var chunkSize = 4 * 1024

var nodeLocalUploadCmd = &cobra.Command{
	Use:   "upload <source> <destination>",
	Short: "Upload file from source to destination",
	Long: `
Uploads a file to a gateway node's local file system using a SQL connection.
`,
	Args: cobra.MinimumNArgs(2),
	RunE: clierrorplus.MaybeShoutError(runUpload),
}

func runUpload(cmd *cobra.Command, args []string) (resErr error) {
	ctx := context.Background()
	conn, err := makeSQLClient(ctx, "cockroach nodelocal", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	source := args[0]
	destination := args[1]
	reader, err := openSourceFile(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	return uploadFile(ctx, conn, reader, destination)
}

func openSourceFile(source string) (io.ReadCloser, error) {
	f, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get source file stats for %s", source)
	}
	if stat.IsDir() {
		return nil, fmt.Errorf("source file %s is a directory, not a file", source)
	}
	return f, nil
}

// escapingReader is an io.Reader that escapes characters from the
// underlying reader for processing by the pgwire COPY protocol.
//
// TODO(ssd): Can we replace this with something that uses the binary
// COPY format.
type escapingReader struct {
	r io.Reader

	readChunk []byte
	buf       []byte
}

func (er *escapingReader) copyBufferTo(b []byte) (int, error) {
	end := len(b)
	if end > len(er.buf) {
		end = len(er.buf)
	}
	n := copy(b, er.buf[:end])
	er.buf = er.buf[end:]
	return n, nil
}

func (er *escapingReader) Read(b []byte) (int, error) {
	// If we have anything left in the buffer from last time,
	// return it now.
	if len(er.buf) > 0 {
		return er.copyBufferTo(b)
	}
	if (er.readChunk) == nil {
		er.readChunk = make([]byte, chunkSize)
	}
	for {
		n, err := er.r.Read(er.readChunk)
		if n > 0 {
			er.buf = appendEscapedText(er.buf, er.readChunk[:n])
			if len(er.buf)+chunkSize > len(b) {
				break
			}
		} else if err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}
	}
	if len(er.buf) > 0 {
		return er.copyBufferTo(b)
	}
	return 0, io.EOF
}

// appendEscapedText escapes the input text for processing by the pgwire COPY
// protocol. The result is appended to the []byte given by buf.
// This implementation is copied from lib/pq.
// https://github.com/lib/pq/blob/8c6de565f76fb5cd40a5c1b8ce583fbc3ba1bd0e/encode.go#L138
func appendEscapedText(buf []byte, text []byte) []byte {
	escapeNeeded := false
	startPos := 0
	var c byte

	// check if we need to escape
	for i := 0; i < len(text); i++ {
		c = text[i]
		if c == '\\' || c == '\n' || c == '\r' || c == '\t' {
			escapeNeeded = true
			startPos = i
			break
		}
	}
	if !escapeNeeded {
		return append(buf, text...)
	}

	// copy till first char to escape, iterate the rest
	result := append(buf, text[:startPos]...)
	for i := startPos; i < len(text); i++ {
		c = text[i]
		switch c {
		case '\\':
			result = append(result, '\\', '\\')
		case '\n':
			result = append(result, '\\', 'n')
		case '\r':
			result = append(result, '\\', 'r')
		case '\t':
			result = append(result, '\\', 't')
		default:
			result = append(result, c)
		}
	}
	return result
}

func uploadFile(
	ctx context.Context, conn clisqlclient.Conn, reader io.Reader, destination string,
) error {
	if err := conn.EnsureConn(ctx); err != nil {
		return err
	}

	ex := conn.GetDriverConn()

	// Construct the nodelocal URI as the destination for the CopyIn stmt.
	nodelocalURL := url.URL{
		Scheme: "nodelocal",
		Host:   "self",
		Path:   destination,
	}
	stmt := sql.CopyInFileStmt(nodelocalURL.String(), sql.CrdbInternalName, sql.NodelocalFileUploadTable)
	if _, err := ex.CopyFrom(ctx, &escapingReader{r: reader}, stmt); err != nil {
		return err
	}

	nodeID, _, _, err := conn.GetServerMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get node id")
	}
	fmt.Printf("successfully uploaded to nodelocal://%s\n", filepath.Join(roachpb.NodeID(nodeID).String(), destination))
	return nil
}

var nodeLocalCmds = []*cobra.Command{
	nodeLocalUploadCmd,
}

var nodeLocalCmd = &cobra.Command{
	Use:   "nodelocal [command]",
	Short: "upload and delete nodelocal files",
	Long:  "Upload and delete files on the gateway node's local file system.",
	RunE:  UsageAndErr,
}

func init() {
	nodeLocalCmd.AddCommand(nodeLocalCmds...)
}

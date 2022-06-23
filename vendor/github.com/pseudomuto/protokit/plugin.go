package protokit

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/plugin"

	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// Plugin describes an interface for running protoc code generator plugins
type Plugin interface {
	Generate(req *plugin_go.CodeGeneratorRequest) (*plugin_go.CodeGeneratorResponse, error)
}

// RunPlugin runs the supplied plugin by reading input from stdin and generating output to stdout.
func RunPlugin(p Plugin) error {
	return RunPluginWithIO(p, os.Stdin, os.Stdout)
}

// RunPluginWithIO runs the supplied plugin using the supplied reader and writer for IO.
func RunPluginWithIO(p Plugin, r io.Reader, w io.Writer) error {
	req, err := readRequest(r)
	if err != nil {
		return err
	}

	resp, err := p.Generate(req)
	if err != nil {
		return err
	}

	return writeResponse(w, resp)
}

func readRequest(r io.Reader) (*plugin_go.CodeGeneratorRequest, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	req := new(plugin_go.CodeGeneratorRequest)
	if err = proto.Unmarshal(data, req); err != nil {
		return nil, err
	}

	if len(req.GetFileToGenerate()) == 0 {
		return nil, fmt.Errorf("no files were supplied to the generator")
	}

	return req, nil
}

func writeResponse(w io.Writer, resp *plugin_go.CodeGeneratorResponse) error {
	data, err := proto.Marshal(resp)
	if err != nil {
		return err
	}

	if _, err := w.Write(data); err != nil {
		return err
	}

	return nil
}

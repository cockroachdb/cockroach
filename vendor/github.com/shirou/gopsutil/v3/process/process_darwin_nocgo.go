//go:build darwin && !cgo
// +build darwin,!cgo

package process

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/internal/common"
)

func (p *Process) CwdWithContext(ctx context.Context) (string, error) {
	return "", common.ErrNotImplementedError
}

func (p *Process) ExeWithContext(ctx context.Context) (string, error) {
	lsof_bin, err := exec.LookPath("lsof")
	if err != nil {
		return "", err
	}
	out, err := invoke.CommandWithContext(ctx, lsof_bin, "-p", strconv.Itoa(int(p.Pid)), "-Fpfn")
	if err != nil {
		return "", fmt.Errorf("bad call to lsof: %s", err)
	}
	txtFound := 0
	lines := strings.Split(string(out), "\n")
	for i := 1; i < len(lines); i++ {
		if lines[i] == "ftxt" {
			txtFound++
			if txtFound == 2 {
				return lines[i-1][1:], nil
			}
		}
	}
	return "", fmt.Errorf("missing txt data returned by lsof")
}

func (p *Process) CmdlineWithContext(ctx context.Context) (string, error) {
	r, err := callPsWithContext(ctx, "command", p.Pid, false, false)
	if err != nil {
		return "", err
	}
	return strings.Join(r[0], " "), err
}

// CmdlineSliceWithContext returns the command line arguments of the process as a slice with each
// element being an argument. Because of current deficiencies in the way that the command
// line arguments are found, single arguments that have spaces in the will actually be
// reported as two separate items. In order to do something better CGO would be needed
// to use the native darwin functions.
func (p *Process) CmdlineSliceWithContext(ctx context.Context) ([]string, error) {
	r, err := callPsWithContext(ctx, "command", p.Pid, false, false)
	if err != nil {
		return nil, err
	}
	return r[0], err
}

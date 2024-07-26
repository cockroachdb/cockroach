package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/go-rod/rod"
	"github.com/stretchr/testify/require"
)

func TestDBConsole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := filepath.Dir(os.Args[0])
	binaryPath := filepath.Join(dir, "../../cmd/cockroach/cockroach_/cockroach")

	cmd := exec.Command(binaryPath, "start-single-node", "--insecure")
	err := cmd.Start()
	require.NoError(t, err)

	browser := rod.New().MustConnect()
	defer browser.MustClose()

	err = browser.IgnoreCertErrors(true)
	require.NoError(t, err)
	page := browser.MustPage("http://localhost:8080").MustWaitLoad()
	element := page.MustElement(".live-nodes").MustWaitVisible()
	text, err := element.Text()
	require.NoError(t, err)
	require.Equal(t, "1", text)

	require.NoError(t, cmd.Process.Signal(syscall.SIGINT))
	_ = cmd.Wait()
}

// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package update

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

func PromptYesNo(msg string) bool {
	fmt.Printf("%s y[default]/n: ", msg)
	var answer string
	_, _ = fmt.Scanln(&answer)
	answer = strings.TrimSpace(answer)

	return answer == "y" || answer == "Y" || answer == ""
}

// SwapBinary attempts to swap the `old` file with the `new` file. Used to
// update a running roachprod binary.
// Note: there is special handling if `new` points to a file ending in `.bak`.
// In this case, it is assumed to be a `revert` operation, in which case we
// do *not* backup the old/current file.
func SwapBinary(old, new string) error {
	destInfo, err := os.Stat(new)

	if err != nil {
		if oserror.IsNotExist(err) {
			return errors.WithDetail(err, "binary does not exist: "+new)
		}
		return err
	}

	if destInfo.IsDir() {
		return errors.Newf("binary path is a directory, not a file: %s", new)
	}

	oldInfo, err := os.Stat(old)
	if err != nil {
		return err
	}

	// Copy the current file permissions to the new binary and ensure it is executable.
	err = os.Chmod(new, oldInfo.Mode())
	if err != nil {
		return err
	}

	// Backup only for upgrading, not when reverting which is assumed if the new binary ends in `.bak`.
	if !strings.HasSuffix(new, ".bak") {
		// Backup the current binary, so that it may be restored via `roachprod update --revert`.
		err = os.Rename(old, old+".bak")
		if err != nil {
			return errors.WithDetail(err, "unable to backup current binary")
		}
	}

	// Move the new binary into place.
	return os.Rename(new, old)
}

// Computes the age of the current binary, relative to the given update time.
func TimeSinceUpdate(updateTime time.Time) (time.Duration, error) {
	currentBinary, err := os.Executable()
	if err != nil {
		return -1, err
	}
	statInfo, err := os.Stat(currentBinary)
	if err != nil {
		return -1, err
	}
	return updateTime.Sub(statInfo.ModTime()), nil
}

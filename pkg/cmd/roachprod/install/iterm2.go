// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package install

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

const splitScript = `
 if (path to frontmost application as text) does not contain "iTerm" then
        return "Can't multi-ssh without iTerm 2 as the active terminal."
 end if
 tell application "iTerm2"
        activate
        tell (create window with default profile)
            set pane_count to %d
            set col_count to round ((pane_count) ^ 0.5) rounding up
            set row_count to round ((pane_count) / col_count) rounding up
            repeat row_count - 1 times
                tell current session
                    split horizontally with default profile
                end tell
            end repeat
            set cur to row_count
            repeat row_count times
                repeat col_count - 1 times
                    if cur is equal to (pane_count) then
                        exit repeat
                    end if
                    tell current session
                        split vertically with default profile
                    end tell
                    set cur to cur + 1
                end repeat
                tell application "System Events" to tell process "iTerm2" to click menu item "Select Pane Below" of menu 1 of menu item "Select Split Pane" of menu 1 of menu bar item "Window" of menu bar 1
            end repeat
            tell current tab
                set node_list to {%s}
                repeat with i from 1 to pane_count
                    tell item i of sessions to write text "roachprod ssh %s:" & item i of node_list
                end repeat
            end tell
        end tell
    end tell
return`

// maybeSplitScreenSSHITerm2 sshs to all of the specified nodes in the
// SyncedCluster in an iTerm2 split-screen configuration if possible. It
// returns true in the first position if it went through with the split-screen
// ssh.
func maybeSplitScreenSSHITerm2(c *SyncedCluster) (bool, error) {
	osascriptPath, err := exec.LookPath("osascript")
	if err != nil {
		return false, err
	}
	nodeStrings := make([]string, len(c.Nodes))
	for i, nodeID := range c.Nodes {
		nodeStrings[i] = fmt.Sprint(nodeID)
	}
	script := fmt.Sprintf(splitScript, len(c.Nodes), strings.Join(nodeStrings, ","), c.Name)
	allArgs := []string{
		"osascript",
		"-e",
		script,
	}
	return true, syscall.Exec(osascriptPath, allArgs, os.Environ())
}

package main

import (
	"bufio"
	"io"
	"regexp"
)

// diff represents a line of difference in a commit.
// the zero value represent non-matching lines in the diff.
// (this is necessary to distinguish an update from adding in one commit and
// removing in the next)
type diff struct {
	importPath, revision string
	added                bool
}

var emptyLine = diff{}

type action int

const (
	add action = iota
	remove
	update
)

type libraryAction struct {
	action               action
	importPath, revision string
}

type cmdAction struct {
	add        bool
	importPath string
}

type playbook struct {
	library []libraryAction // updates to libraries in gopath
	cmd     []cmdAction     // updates to cmds that should be built
}

const (
	importPathExpr = `[\w.]+\.\w+/[\w/.-]+`
	libLineExpr    = `[+-](` + importPathExpr + `) (\w+)`
	cmdLineExpr    = `[+-]cmd (` + importPathExpr + `)`
)

var (
	libLineRegex = regexp.MustCompile(libLineExpr)
	cmdLineRegex = regexp.MustCompile(cmdLineExpr)
)

func readDiffLines(reader io.Reader) []diff {
	// Get the list of diffs from the commit log.
	var (
		diffs   []diff
		scanner = bufio.NewScanner(reader)
	)
	for scanner.Scan() {
		var txt = scanner.Text()
		if matches := libLineRegex.FindStringSubmatch(txt); matches != nil {
			diffs = append(diffs, diff{
				importPath: matches[1],
				revision:   matches[2],
				added:      txt[0] == '+',
			})
		} else if matches := cmdLineRegex.FindStringSubmatch(txt); matches != nil {
			diffs = append(diffs, diff{
				importPath: matches[1],
				revision:   "cmd",
				added:      txt[0] == '+',
			})
		} else {
			diffs = append(diffs, emptyLine)
		}
	}
	return diffs
}

func processDiffBlock(diffs []diff) []libraryAction {
	// Assume:
	// - An import path appears once or twice in a block. If twice, it was updated.
	var importPathActions = make(map[string]libraryAction)
	for _, this := range diffs {
		// Calculate the new action for this import path.
		// (Potentially this updates a previously recorded action from e.g. add to update)
		if existing, ok := importPathActions[this.importPath]; ok {
			if existing.action == update {
				panic("unexpected: found import path " + this.importPath + " > 2 times in the diff")
			}
			importPathActions[this.importPath] = newUpdate(existing, this)
		} else {
			importPathActions[this.importPath] = newAddOrRemove(this)
		}
	}

	// Build all the library actions
	var result []libraryAction
	for _, action := range importPathActions {
		result = append(result, action)
	}
	return result
}

func buildPlaybook(diffs []diff) playbook {
	// Convert diffs into actions.  Since they may touch the same lines over
	// multiple commits, keep track of import paths that we've added commands for,
	// and only add the first.
	var (
		book            playbook
		block           []diff
		seenImportPaths = make(map[string]struct{})
	)
	for _, this := range append(diffs, emptyLine) {
		switch {
		case this == emptyLine:
			// Consume the past block of diffs. Blocks of diffs are separated by emptyLine.
			// Assume:
			// - Blocks are processed in reverse chronological order. If we've seen an
			//   import path in a previous block, ignore it entirely.
			if block == nil {
				continue
			}
			for _, libAction := range processDiffBlock(block) {
				if _, ok := seenImportPaths[libAction.importPath]; ok {
					continue
				}
				seenImportPaths[libAction.importPath] = struct{}{}
				book.library = append(book.library, libAction)
			}
			block = nil
		case this.revision == "cmd":
			book.cmd = append(book.cmd, cmdAction{this.added, this.importPath})
		default:
			block = append(block, this)
		}
	}
	return book
}

func newUpdate(a libraryAction, b diff) libraryAction {
	if b.added {
		return newCommand(update, b)
	}
	a.action = update
	return a
}

func newAddOrRemove(d diff) libraryAction {
	var action = add
	if !d.added {
		action = remove
	}
	return newCommand(action, d)
}

func newCommand(a action, d diff) libraryAction {
	return libraryAction{
		action:     a,
		importPath: d.importPath,
		revision:   d.revision,
	}
}

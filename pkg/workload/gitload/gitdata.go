// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gitload

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os/exec"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// commitMeta holds parsed metadata for a single git commit.
type commitMeta struct {
	hash           string
	treeHash       string
	authorName     string
	authorEmail    string
	authorDate     string
	committerName  string
	committerEmail string
	committerDate  string
	message        string
	parents        []string
}

// diffEntry represents a single file change in a commit.
type diffEntry struct {
	op      byte   // 'A' (add), 'M' (modify), 'D' (delete), 'R' (rename)
	path    string // new path (or only path for A/M/D)
	oldPath string // old path (only for R)
	mode    string // file mode
}

// runGit executes a git command in the given repo and returns stdout.
func runGit(ctx context.Context, repoPath string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = repoPath
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, errors.Wrapf(err, "git %s: %s", strings.Join(args, " "), stderr.String())
	}
	return out, nil
}

// getCommitMeta parses commit metadata using NUL-delimited git log output.
func getCommitMeta(ctx context.Context, repoPath string, hash string) (commitMeta, error) {
	// Format: hash, tree, author name, author email, author date,
	// committer name, committer email, committer date, parents, message
	// All NUL-delimited.
	format := "%H%x00%T%x00%an%x00%ae%x00%aI%x00%cn%x00%ce%x00%cI%x00%P%x00%s"
	out, err := runGit(ctx, repoPath, "log", "-1", "--format="+format, hash)
	if err != nil {
		return commitMeta{}, err
	}
	parts := strings.Split(strings.TrimRight(string(out), "\n"), "\x00")
	if len(parts) != 10 {
		return commitMeta{}, errors.Newf(
			"unexpected git log output: got %d fields, want 10", len(parts),
		)
	}
	cm := commitMeta{
		hash:           parts[0],
		treeHash:       parts[1],
		authorName:     parts[2],
		authorEmail:    parts[3],
		authorDate:     parts[4],
		committerName:  parts[5],
		committerEmail: parts[6],
		committerDate:  parts[7],
		message:        parts[9],
	}
	if parts[8] != "" {
		cm.parents = strings.Split(parts[8], " ")
	}
	return cm, nil
}

// getDiffTree returns the file changes for a commit by diffing against the
// first parent. For root commits, it diffs against an empty tree.
func getDiffTree(
	ctx context.Context, repoPath string, hash string, parents []string,
) ([]diffEntry, error) {
	var args []string
	if len(parents) == 0 {
		// Root commit: diff against empty tree. The --root flag is required
		// so that git emits the full tree as additions; without it, diff-tree
		// on a parentless commit produces no output.
		args = []string{"diff-tree", "-r", "-M", "-z", "--root", "--no-commit-id", hash}
	} else {
		// Diff against first parent.
		args = []string{"diff-tree", "-r", "-M", "-z", "--no-commit-id", parents[0], hash}
	}
	out, err := runGit(ctx, repoPath, args...)
	if err != nil {
		return nil, err
	}
	return parseDiffTreeOutput(out)
}

// parseDiffTreeOutput parses the NUL-delimited output of git diff-tree -z.
func parseDiffTreeOutput(out []byte) ([]diffEntry, error) {
	if len(out) == 0 {
		return nil, nil
	}
	// Split on NUL bytes and process fields. The format is:
	// :oldmode newmode oldhash newhash status\0path\0
	// For renames: :oldmode newmode oldhash newhash Rnn\0oldpath\0newpath\0
	fields := strings.Split(strings.TrimRight(string(out), "\x00"), "\x00")
	var entries []diffEntry
	i := 0
	for i < len(fields) {
		field := fields[i]
		if !strings.HasPrefix(field, ":") {
			return nil, errors.Newf("expected diff-tree header starting with ':', got %q", field)
		}
		// Parse header: ":oldmode newmode oldhash newhash status"
		parts := strings.Fields(field[1:])
		if len(parts) < 5 {
			return nil, errors.Newf("unexpected diff-tree header: %q", field)
		}
		status := parts[4]
		mode := parts[1]
		i++
		if i >= len(fields) {
			return nil, errors.New("unexpected end of diff-tree output")
		}
		var entry diffEntry
		entry.mode = mode
		switch {
		case status == "A":
			entry.op = 'A'
			entry.path = fields[i]
			i++
		case status == "M":
			entry.op = 'M'
			entry.path = fields[i]
			i++
		case status == "D":
			entry.op = 'D'
			entry.path = fields[i]
			i++
		case strings.HasPrefix(status, "R"):
			entry.op = 'R'
			entry.oldPath = fields[i]
			i++
			if i >= len(fields) {
				return nil, errors.New("unexpected end of diff-tree output for rename")
			}
			entry.path = fields[i]
			i++
		default:
			// Skip unknown status types (C for copy, etc.).
			entry.op = status[0]
			entry.path = fields[i]
			i++
			if len(status) > 0 && (status[0] == 'C' || status[0] == 'R') {
				// Copy/rename have two paths.
				if i < len(fields) {
					i++
				}
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// readBlob reads file content at a specific commit via git show.
func readBlob(
	ctx context.Context, repoPath string, commitHash string, filePath string,
) ([]byte, error) {
	return runGit(ctx, repoPath, "show", fmt.Sprintf("%s:%s", commitHash, filePath))
}

// gitLsTree returns the set of file paths in the tree at the given commit.
func gitLsTree(
	ctx context.Context, repoPath string, commitHash string,
) (map[string]struct{}, error) {
	out, err := runGit(ctx, repoPath, "ls-tree", "-r", "-z", "--name-only", commitHash)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]struct{})
	if len(out) == 0 {
		return paths, nil
	}
	for _, p := range strings.Split(strings.TrimRight(string(out), "\x00"), "\x00") {
		if p != "" {
			paths[p] = struct{}{}
		}
	}
	return paths, nil
}

// loadCommitDAG loads the commit DAG via git rev-list --parents HEAD.
// Returns a map from commit hash to its parent hashes.
func loadCommitDAG(ctx context.Context, repoPath string) (map[string][]string, error) {
	out, err := runGit(ctx, repoPath, "rev-list", "--parents", "HEAD")
	if err != nil {
		return nil, err
	}
	dag := make(map[string][]string)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		hash := parts[0]
		var parents []string
		if len(parts) > 1 {
			parents = parts[1:]
		}
		dag[hash] = parents
	}
	return dag, nil
}

// randomTopoSort produces a topological ordering of the DAG using Kahn's
// algorithm with a seeded RNG for tie-breaking. This ensures deterministic
// but varied orderings when the seed changes.
func randomTopoSort(dag map[string][]string, seed int64) ([]string, error) {
	// Build in-degree map and reverse adjacency (child -> parents becomes
	// parent -> children for the forward graph).
	inDegree := make(map[string]int, len(dag))
	children := make(map[string][]string, len(dag))
	for hash := range dag {
		if _, ok := inDegree[hash]; !ok {
			inDegree[hash] = 0
		}
		for _, parent := range dag[hash] {
			children[parent] = append(children[parent], hash)
			inDegree[hash]++
		}
	}

	// Collect nodes with in-degree 0 (roots).
	var queue []string
	for hash, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, hash)
		}
	}

	rng := rand.New(rand.NewPCG(uint64(seed), 0))
	var result []string
	for len(queue) > 0 {
		// Pick a random element from the queue for tie-breaking.
		idx := rng.IntN(len(queue))
		hash := queue[idx]
		queue[idx] = queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		result = append(result, hash)
		for _, child := range children[hash] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(result) != len(dag) {
		return nil, errors.Newf(
			"topological sort incomplete: processed %d of %d nodes (cycle?)",
			len(result), len(dag),
		)
	}
	return result, nil
}

// commitCount returns the number of commits reachable from HEAD.
func commitCount(ctx context.Context, repoPath string) (int, error) {
	out, err := runGit(ctx, repoPath, "rev-list", "--count", "HEAD")
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0, errors.Wrap(err, "parsing commit count")
	}
	return n, nil
}

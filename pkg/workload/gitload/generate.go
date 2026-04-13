// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gitload

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

// generateRepo creates a deterministic synthetic git repo at repoPath.
// If the repo already exists and has the expected number of commits, it is
// reused. The repo includes occasional branches and merges for DAG complexity.
func generateRepo(ctx context.Context, repoPath string, numCommits int, seed int64) error {
	// Check if repo already exists with enough commits.
	if info, err := os.Stat(filepath.Join(repoPath, ".git")); err == nil && info.IsDir() {
		n, err := commitCount(ctx, repoPath)
		if err == nil && n >= numCommits {
			fmt.Printf("reusing existing repo at %s with %d commits\n", repoPath, n)
			return nil
		}
	}

	// Remove any partial repo.
	if err := os.RemoveAll(repoPath); err != nil {
		return errors.Wrap(err, "cleaning repo path")
	}
	if err := os.MkdirAll(repoPath, 0755); err != nil {
		return errors.Wrap(err, "creating repo directory")
	}

	// Initialize git repo.
	if _, err := runGit(ctx, repoPath, "init"); err != nil {
		return err
	}
	if _, err := runGit(ctx, repoPath, "config", "user.email", "gitload@cockroachlabs.com"); err != nil {
		return err
	}
	if _, err := runGit(ctx, repoPath, "config", "user.name", "gitload"); err != nil {
		return err
	}

	rng := rand.New(rand.NewPCG(uint64(seed), 0))

	// Track existing files for modify/delete/rename operations.
	var existingFiles []string

	// Counters for branch/merge complexity.
	commitsSinceLastBranch := 0
	onBranch := false
	branchIdx := 0
	mainCommit := 0

	for i := 0; i < numCommits; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Every ~20 commits, create a branch, add a few commits, then merge.
		if !onBranch && commitsSinceLastBranch >= 20 && i < numCommits-5 && rng.IntN(3) == 0 {
			branchName := fmt.Sprintf("feature-%d", branchIdx)
			branchIdx++
			if _, err := runGit(ctx, repoPath, "checkout", "-b", branchName); err != nil {
				return err
			}
			onBranch = true
			commitsSinceLastBranch = 0
			mainCommit = i

			// Add 2-4 commits on the branch.
			branchCommits := 2 + rng.IntN(3)
			for j := 0; j < branchCommits && i < numCommits; j++ {
				if err := makeCommit(ctx, repoPath, rng, &existingFiles, i); err != nil {
					return err
				}
				i++
			}

			// Merge back to main.
			if _, err := runGit(ctx, repoPath, "checkout", "main"); err != nil {
				// Try "master" if "main" doesn't exist.
				if _, err2 := runGit(ctx, repoPath, "checkout", "master"); err2 != nil {
					return errors.Wrap(err, "checking out main branch")
				}
			}
			mergeMsg := fmt.Sprintf("Merge feature-%d (commits %d-%d)", branchIdx-1, mainCommit, i-1)
			if _, err := runGit(ctx, repoPath, "merge", "--no-ff", "-m", mergeMsg, branchName); err != nil {
				return errors.Wrapf(err, "merging branch %s", branchName)
			}
			if _, err := runGit(ctx, repoPath, "branch", "-d", branchName); err != nil {
				return errors.Wrapf(err, "deleting branch %s", branchName)
			}
			onBranch = false
			continue
		}

		if err := makeCommit(ctx, repoPath, rng, &existingFiles, i); err != nil {
			return err
		}
		commitsSinceLastBranch++

		if i > 0 && i%100 == 0 {
			fmt.Printf("generated %d/%d commits\n", i, numCommits)
		}
	}

	fmt.Printf("finished generating repo with %d requested commits at %s\n", numCommits, repoPath)
	return nil
}

// makeCommit creates a single commit with 1-5 random file changes.
func makeCommit(
	ctx context.Context, repoPath string, rng *rand.Rand, existingFiles *[]string, commitIdx int,
) error {
	numChanges := 1 + rng.IntN(5)
	for j := 0; j < numChanges; j++ {
		action := chooseAction(rng, len(*existingFiles))
		switch action {
		case "add":
			dir := fmt.Sprintf("dir_%d", rng.IntN(10))
			name := fmt.Sprintf("file_%d.txt", rng.IntN(50))
			path := filepath.Join(dir, name)
			fullPath := filepath.Join(repoPath, dir)
			if err := os.MkdirAll(fullPath, 0755); err != nil {
				return errors.Wrap(err, "creating directory")
			}
			content := generateContent(rng, commitIdx)
			if err := os.WriteFile(filepath.Join(repoPath, path), content, 0644); err != nil {
				return errors.Wrap(err, "writing file")
			}
			if _, err := runGit(ctx, repoPath, "add", path); err != nil {
				return err
			}
			*existingFiles = append(*existingFiles, path)

		case "modify":
			if len(*existingFiles) == 0 {
				continue
			}
			idx := rng.IntN(len(*existingFiles))
			path := (*existingFiles)[idx]
			content := generateContent(rng, commitIdx)
			if err := os.WriteFile(filepath.Join(repoPath, path), content, 0644); err != nil {
				return errors.Wrap(err, "modifying file")
			}
			if _, err := runGit(ctx, repoPath, "add", path); err != nil {
				return err
			}

		case "delete":
			if len(*existingFiles) == 0 {
				continue
			}
			idx := rng.IntN(len(*existingFiles))
			path := (*existingFiles)[idx]
			if _, err := runGit(ctx, repoPath, "rm", "-f", path); err != nil {
				// File might already be deleted on a branch, skip.
				continue
			}
			// Remove from tracking list.
			(*existingFiles)[idx] = (*existingFiles)[len(*existingFiles)-1]
			*existingFiles = (*existingFiles)[:len(*existingFiles)-1]

		case "rename":
			if len(*existingFiles) == 0 {
				continue
			}
			idx := rng.IntN(len(*existingFiles))
			oldPath := (*existingFiles)[idx]
			dir := fmt.Sprintf("dir_%d", rng.IntN(10))
			name := fmt.Sprintf("file_%d.txt", rng.IntN(50))
			newPath := filepath.Join(dir, name)
			if oldPath == newPath {
				continue
			}
			newDir := filepath.Join(repoPath, dir)
			if err := os.MkdirAll(newDir, 0755); err != nil {
				return errors.Wrap(err, "creating directory for rename")
			}
			if _, err := runGit(ctx, repoPath, "mv", "-f", oldPath, newPath); err != nil {
				continue
			}
			(*existingFiles)[idx] = newPath
		}
	}

	msg := fmt.Sprintf("commit %d", commitIdx)
	// Check if there are staged changes before committing.
	if _, err := runGit(ctx, repoPath, "diff", "--cached", "--quiet"); err == nil {
		// No changes staged, create an empty change to ensure a commit.
		path := fmt.Sprintf("dir_0/marker_%d.txt", commitIdx)
		if err := os.MkdirAll(filepath.Join(repoPath, "dir_0"), 0755); err != nil {
			return errors.Wrap(err, "creating marker directory")
		}
		content := generateContent(rng, commitIdx)
		if err := os.WriteFile(filepath.Join(repoPath, path), content, 0644); err != nil {
			return errors.Wrap(err, "writing marker file")
		}
		if _, err := runGit(ctx, repoPath, "add", path); err != nil {
			return err
		}
		*existingFiles = append(*existingFiles, path)
	}
	if _, err := runGit(ctx, repoPath, "commit", "-m", msg); err != nil {
		return err
	}
	return nil
}

// chooseAction picks a random file operation based on weights and whether files
// exist for modification.
func chooseAction(rng *rand.Rand, numExisting int) string {
	if numExisting == 0 {
		return "add"
	}
	r := rng.IntN(100)
	switch {
	case r < 50:
		return "add"
	case r < 80:
		return "modify"
	case r < 90:
		return "delete"
	default:
		return "rename"
	}
}

// generateContent produces deterministic random content based on the RNG state.
func generateContent(rng *rand.Rand, commitIdx int) []byte {
	size := 64 + rng.IntN(512)
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(32 + rng.IntN(95)) // printable ASCII
	}
	return buf
}

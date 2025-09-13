package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ShaComparer handles comparison of test results between two Git SHAs
type ShaComparer struct {
	RepoRoot string
	TempDir  string
}

// NewShaComparer creates a new SHA comparer
func NewShaComparer() (*ShaComparer, error) {
	// Find git repo root
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to find git repo root: %w", err)
	}
	repoRoot := strings.TrimSpace(string(output))

	// Create temp directory for SHA comparisons within testdata
	tempDir := filepath.Join(repoRoot, "pkg/kv/kvserver/asim/tests/testdata", "asim_sha_compare_temp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	return &ShaComparer{
		RepoRoot: repoRoot,
		TempDir:  tempDir,
	}, nil
}

// GenerateTestDataForSha generates test data for a specific SHA and stores it
func (sc *ShaComparer) GenerateTestDataForSha(sha string) error {
	fmt.Printf("Generating test data for SHA: %s\n", sha)

	// Resolve partial SHA to full SHA if needed
	fullSha, err := sc.resolveSha(sha)
	if err != nil {
		return fmt.Errorf("failed to resolve SHA %s: %w", sha, err)
	}

	// Create directory for this SHA
	shaDir := filepath.Join(sc.TempDir, fullSha)
	if err := os.MkdirAll(shaDir, 0755); err != nil {
		return fmt.Errorf("failed to create SHA directory: %w", err)
	}

	// Check if data already exists for this SHA
	generatedPath := filepath.Join(shaDir, "generated")
	if _, err := os.Stat(generatedPath); err == nil {
		fmt.Printf("Test data already exists for SHA %s, skipping generation\n", fullSha)
		return nil
	}

	// Get current HEAD to restore later (safer than branch name)
	currentHead, err := sc.getCurrentHead()
	if err != nil {
		return fmt.Errorf("failed to get current HEAD: %w", err)
	}

	// Check if working directory is clean before proceeding
	if !sc.isWorkingDirectoryClean() {
		fmt.Printf("Working directory has changes, stashing them...\n")
		if err := sc.runGitCommand("stash", "push", "-m", "Temporary stash for SHA comparison"); err != nil {
			return fmt.Errorf("failed to stash changes: %w", err)
		}
		defer func() {
			fmt.Printf("Restoring stashed changes...\n")
			if err := sc.runGitCommand("stash", "pop"); err != nil {
				log.Printf("Warning: Failed to restore stashed changes: %v", err)
			}
		}()
	}

	// Checkout the target SHA
	if err := sc.runGitCommand("checkout", fullSha); err != nil {
		return fmt.Errorf("failed to checkout SHA %s: %w", fullSha, err)
	}

	// Ensure we restore the original state
	defer func() {
		fmt.Printf("Restoring original HEAD (%s)...\n", currentHead[:12])
		if err := sc.runGitCommand("checkout", currentHead); err != nil {
			log.Printf("Error: Failed to restore HEAD %s: %v", currentHead, err)
		}
	}()

	// Clean any existing generated files to ensure fresh test run
	generatedDir := filepath.Join(sc.RepoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")
	if err := sc.cleanGeneratedDir(generatedDir); err != nil {
		log.Printf("Warning: Failed to clean generated directory: %v", err)
	}

	// Run the test to generate data
	testCmd := exec.Command("./dev", "test", "pkg/kv/kvserver/asim/tests", "-f", "TestDataDriven", "--ignore-cache", "--rewrite", "--", "--test_env", "COCKROACH_RUN_ASIM_TESTS=true")
	testCmd.Dir = sc.RepoRoot
	testCmd.Stdout = os.Stdout
	testCmd.Stderr = os.Stderr

	fmt.Printf("Running test command for SHA %s...\n", fullSha)
	if err := testCmd.Run(); err != nil {
		return fmt.Errorf("test command failed for SHA %s: %w", fullSha, err)
	}

	// Copy generated files to SHA directory
	if err := sc.copyDir(generatedDir, generatedPath); err != nil {
		return fmt.Errorf("failed to copy generated files: %w", err)
	}

	fmt.Printf("Successfully generated test data for SHA %s\n", fullSha)
	return nil
}

// CompareShAs generates test data for two SHAs and prepares for comparison
func (sc *ShaComparer) CompareShAs(sha1, sha2 string) error {
	fmt.Printf("Starting comparison between SHAs: %s and %s\n", sha1, sha2)

	// Generate data for both SHAs
	if err := sc.GenerateTestDataForSha(sha1); err != nil {
		return fmt.Errorf("failed to generate data for SHA %s: %w", sha1, err)
	}

	if err := sc.GenerateTestDataForSha(sha2); err != nil {
		return fmt.Errorf("failed to generate data for SHA %s: %w", sha2, err)
	}

	fmt.Printf("Successfully prepared comparison data for SHAs %s and %s\n", sha1, sha2)
	fmt.Printf("Data stored in: %s\n", sc.TempDir)
	return nil
}

// ShaInfo represents SHA with commit information
type ShaInfo struct {
	Sha     string `json:"sha"`
	Short   string `json:"short"`
	Subject string `json:"subject"`
	Author  string `json:"author"`
	Date    string `json:"date"`
}

// GetComparisons returns the list of available SHA comparisons with commit info
func (sc *ShaComparer) GetComparisons() ([]ShaInfo, error) {
	entries, err := os.ReadDir(sc.TempDir)
	if err != nil {
		return nil, err
	}

	var shaInfos []ShaInfo
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) >= 7 { // SHA should be at least 7 chars
			sha := entry.Name()
			info, err := sc.getCommitInfo(sha)
			if err != nil {
				log.Printf("Warning: Failed to get commit info for %s: %v", sha, err)
				// Still include it with just the SHA
				shaInfos = append(shaInfos, ShaInfo{
					Sha:     sha,
					Short:   sha[:7],
					Subject: "Unknown commit",
					Author:  "",
					Date:    "",
				})
			} else {
				shaInfos = append(shaInfos, info)
			}
		}
	}
	return shaInfos, nil
}

// GetRecentCommits returns the most recent commits from the current branch
func (sc *ShaComparer) GetRecentCommits(count int) ([]ShaInfo, error) {
	cmd := exec.Command("git", "log", "-n", fmt.Sprintf("%d", count), "--pretty=format:%H|%h|%s")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get recent commits: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var commits []ShaInfo

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) < 3 {
			continue
		}

		commits = append(commits, ShaInfo{
			Sha:     parts[0],
			Short:   parts[1],
			Subject: parts[2],
			Author:  "",
			Date:    "",
		})
	}

	return commits, nil
}

// Helper functions

func (sc *ShaComparer) getCommitInfo(sha string) (ShaInfo, error) {
	// Get single commit information
	cmd := exec.Command("git", "log", "-1", "--pretty=format:%H|%h|%s|%an|%ad", "--date=short", sha)
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return ShaInfo{}, fmt.Errorf("failed to get commit info: %w", err)
	}

	parts := strings.Split(strings.TrimSpace(string(output)), "|")
	if len(parts) < 5 {
		return ShaInfo{}, fmt.Errorf("unexpected git log output format")
	}

	return ShaInfo{
		Sha:     parts[0],
		Short:   parts[1],
		Subject: parts[2],
		Author:  parts[3],
		Date:    parts[4],
	}, nil
}

func (sc *ShaComparer) resolveSha(partialSha string) (string, error) {
	// If it's already a full SHA (40 chars), return as-is
	if len(partialSha) == 40 {
		return partialSha, nil
	}

	// Try to resolve partial SHA using git
	cmd := exec.Command("git", "rev-parse", partialSha)
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("SHA %s not found in git repository", partialSha)
	}

	fullSha := strings.TrimSpace(string(output))

	// Validate it's a proper SHA
	if len(fullSha) != 40 {
		return "", fmt.Errorf("invalid SHA resolved: %s", fullSha)
	}

	return fullSha, nil
}

func (sc *ShaComparer) getCurrentHead() (string, error) {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (sc *ShaComparer) getCurrentBranch() (string, error) {
	cmd := exec.Command("git", "branch", "--show-current")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (sc *ShaComparer) isWorkingDirectoryClean() bool {
	cmd := exec.Command("git", "status", "--porcelain")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		log.Printf("Warning: Failed to check git status: %v", err)
		return false
	}
	return len(strings.TrimSpace(string(output))) == 0
}

func (sc *ShaComparer) cleanGeneratedDir(dir string) error {
	// Only clean files that are gitignored or untracked to avoid data loss
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(dir, entry.Name())
			if err := sc.cleanGeneratedDir(subDir); err != nil {
				log.Printf("Warning: Failed to clean subdirectory %s: %v", subDir, err)
			}
		} else if strings.HasSuffix(entry.Name(), ".json") {
			// Only remove JSON files to be safe
			filePath := filepath.Join(dir, entry.Name())
			if err := os.Remove(filePath); err != nil {
				log.Printf("Warning: Failed to remove %s: %v", filePath, err)
			}
		}
	}
	return nil
}

func (sc *ShaComparer) runGitCommand(args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = sc.RepoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (sc *ShaComparer) copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
			return err
		}

		dstFile, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer dstFile.Close()

		_, err = srcFile.WriteTo(dstFile)
		return err
	})
}

// Cleanup removes the temporary directory
func (sc *ShaComparer) Cleanup() error {
	return os.RemoveAll(sc.TempDir)
}

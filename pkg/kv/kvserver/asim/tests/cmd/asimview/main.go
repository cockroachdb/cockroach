package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

//go:embed viewer.html
var viewerHTML string

//go:embed sha_compare.html
var shaCompareHTML string

type FileInfo struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	TestName string `json:"testName"`
}

type FileComparison struct {
	Path        string `json:"path"`
	Name        string `json:"name"`
	TestName    string `json:"testName"`
	HasDiff     bool   `json:"hasDiff"`
	Identical   bool   `json:"identical"`
	OnlyInSha1  bool   `json:"onlyInSha1"`
	OnlyInSha2  bool   `json:"onlyInSha2"`
}

func main() {
	var port int
	var shaCompare bool
	flag.IntVar(&port, "port", 8080, "Port to serve on")
	flag.BoolVar(&shaCompare, "sha-compare", false, "Enable SHA comparison mode")
	flag.Parse()

	if shaCompare {
		// SHA comparison mode
		fmt.Printf("SHA Comparison Viewer available at: http://localhost:%d\n", port)
		setupShaComparisonRoutes()
	} else {
		// Regular file viewing mode
		dir := flag.Arg(0)
		if dir == "" {
			// Find git repo root and default to generated testdata
			cmd := exec.Command("git", "rev-parse", "--show-toplevel")
			output, err := cmd.Output()
			if err != nil {
				log.Fatal("Failed to find git repo root:", err)
			}
			repoRoot := strings.TrimSpace(string(output))
			dir = filepath.Join(repoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")
		}

		absDir, err := filepath.Abs(dir)
		if err != nil {
			log.Fatal("Failed to resolve directory:", err)
		}

		if _, err := os.Stat(absDir); os.IsNotExist(err) {
			log.Fatalf("Directory does not exist: %s", absDir)
		}

		fmt.Printf("Serving files from: %s\n", absDir)
		fmt.Printf("Viewer available at: http://localhost:%d\n", port)

		http.HandleFunc("/", serveViewer)
		http.HandleFunc("/api/files", makeFileListHandler(absDir))
		http.HandleFunc("/api/file/", makeFileHandler(absDir))
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func serveViewer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(viewerHTML))
}

func makeFileListHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var files []FileInfo

		err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(path, ".json") {
				relPath, _ := filepath.Rel(baseDir, path)

				// Extract test name from file name only (ignore directories)
				baseName := filepath.Base(path)
				testName := strings.TrimSuffix(baseName, ".json")

				files = append(files, FileInfo{
					Path:     relPath,
					Name:     baseName,
					TestName: testName,
				})
			}
			return nil
		})

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(files)
	}
}

func makeFileHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract file path from URL
		filePath := strings.TrimPrefix(r.URL.Path, "/api/file/")

		// Prevent directory traversal
		cleanPath := filepath.Clean(filePath)
		if strings.Contains(cleanPath, "..") {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		fullPath := filepath.Join(baseDir, cleanPath)

		// Check if file exists and is within baseDir
		if !strings.HasPrefix(fullPath, baseDir) {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		file, err := os.Open(fullPath)
		if err != nil {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		defer file.Close()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		io.Copy(w, file)
	}
}

// SHA comparison handlers

type ShaComparisonRequest struct {
	Sha1 string `json:"sha1"`
	Sha2 string `json:"sha2"`
}

var shaComparer *ShaComparer

func setupShaComparisonRoutes() {
	var err error
	shaComparer, err = NewShaComparer()
	if err != nil {
		log.Fatal("Failed to initialize SHA comparer:", err)
	}

	http.HandleFunc("/", serveShaCompareViewer)
	http.HandleFunc("/api/sha-comparisons", getShaComparisons)
	http.HandleFunc("/api/recent-commits", getRecentCommits)
	http.HandleFunc("/api/generate-comparison", generateComparison)
	http.HandleFunc("/api/comparison-files/", getComparisonFiles)
	http.HandleFunc("/api/sha-file/", getShaFile)
	http.HandleFunc("/api/compare-files/", compareFiles)
}

func serveShaCompareViewer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(shaCompareHTML))
}

func getShaComparisons(w http.ResponseWriter, r *http.Request) {
	shaInfos, err := shaComparer.GetComparisons()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(shaInfos)
}

func getRecentCommits(w http.ResponseWriter, r *http.Request) {
	commits, err := shaComparer.GetRecentCommits(5)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(commits)
}

func generateComparison(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ShaComparisonRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Sha1 == "" || req.Sha2 == "" {
		http.Error(w, "Both sha1 and sha2 are required", http.StatusBadRequest)
		return
	}

	err := shaComparer.CompareShAs(req.Sha1, req.Sha2)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func getComparisonFiles(w http.ResponseWriter, r *http.Request) {
	// Extract sha1/sha2 from URL path like /api/comparison-files/sha1/sha2
	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/comparison-files/"), "/")
	if len(pathParts) != 2 {
		http.Error(w, "Invalid path format", http.StatusBadRequest)
		return
	}

	sha1, sha2 := pathParts[0], pathParts[1]
	
	// Resolve partial SHAs to full SHAs if needed
	fullSha1, err := shaComparer.resolveSha(sha1)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid SHA1: %s", sha1), http.StatusBadRequest)
		return
	}
	
	fullSha2, err := shaComparer.resolveSha(sha2)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid SHA2: %s", sha2), http.StatusBadRequest)
		return
	}
	
	// Get files from the first SHA directory
	sha1Dir := filepath.Join(shaComparer.TempDir, fullSha1, "generated")
	var files []FileInfo

	err = filepath.Walk(sha1Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".json") {
			relPath, _ := filepath.Rel(sha1Dir, path)
			baseName := filepath.Base(path)
			
			// Use the directory name as the test name instead of just the filename
			// e.g., for "example_rebalancing/example_rebalancing_default_1.json" 
			// use "example_rebalancing" as the test name
			dir := filepath.Dir(relPath)
			if dir == "." {
				// If no subdirectory, use filename without extension
				testName := strings.TrimSuffix(baseName, ".json")
				files = append(files, FileInfo{
					Path:     relPath,
					Name:     baseName,
					TestName: testName,
				})
			} else {
				// Use directory name as test name
				testName := dir
				displayName := fmt.Sprintf("%s (%s)", testName, baseName)
				
				// Check if corresponding file exists in sha2
				sha2File := filepath.Join(shaComparer.TempDir, fullSha2, "generated", relPath)
				if _, err := os.Stat(sha2File); err == nil {
					files = append(files, FileInfo{
						Path:     relPath,
						Name:     displayName,
						TestName: testName,
					})
				}
			}
		}
		return nil
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(files)
}

func getShaFile(w http.ResponseWriter, r *http.Request) {
	// Extract sha/filename from URL path like /api/sha-file/sha/filename.json
	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/sha-file/"), "/")
	if len(pathParts) < 2 {
		http.Error(w, "Invalid path format", http.StatusBadRequest)
		return
	}

	sha := pathParts[0]
	filename := strings.Join(pathParts[1:], "/")

	// Resolve partial SHA to full SHA if needed
	fullSha, err := shaComparer.resolveSha(sha)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid SHA: %s", sha), http.StatusBadRequest)
		return
	}

	// Prevent directory traversal
	cleanFilename := filepath.Clean(filename)
	if strings.Contains(cleanFilename, "..") {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(shaComparer.TempDir, fullSha, "generated", cleanFilename)

	// Check if file exists and is within expected directory
	expectedBase := filepath.Join(shaComparer.TempDir, fullSha, "generated")
	if !strings.HasPrefix(filePath, expectedBase) {
		http.Error(w, "Invalid file path", http.StatusBadRequest)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	io.Copy(w, file)
}

func compareFiles(w http.ResponseWriter, r *http.Request) {
	// Extract sha1/sha2 from URL path like /api/compare-files/sha1/sha2
	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/compare-files/"), "/")
	if len(pathParts) != 2 {
		http.Error(w, "Invalid path format", http.StatusBadRequest)
		return
	}

	sha1, sha2 := pathParts[0], pathParts[1]
	
	// Resolve partial SHAs to full SHAs if needed
	fullSha1, err := shaComparer.resolveSha(sha1)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid SHA1: %s", sha1), http.StatusBadRequest)
		return
	}
	
	fullSha2, err := shaComparer.resolveSha(sha2)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid SHA2: %s", sha2), http.StatusBadRequest)
		return
	}
	
	// Get files from both SHA directories
	sha1Dir := filepath.Join(shaComparer.TempDir, fullSha1, "generated")
	sha2Dir := filepath.Join(shaComparer.TempDir, fullSha2, "generated")
	
	fileComparisons := make(map[string]*FileComparison)
	
	// Walk through SHA1 files
	err = filepath.Walk(sha1Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".json") {
			relPath, _ := filepath.Rel(sha1Dir, path)
			baseName := filepath.Base(path)
			
			dir := filepath.Dir(relPath)
			var testName string
			var displayName string
			
			if dir == "." {
				testName = strings.TrimSuffix(baseName, ".json")
				displayName = baseName
			} else {
				testName = dir
				displayName = fmt.Sprintf("%s (%s)", testName, baseName)
			}
			
			// Check if corresponding file exists in sha2
			sha2File := filepath.Join(sha2Dir, relPath)
			
			comparison := &FileComparison{
				Path:     relPath,
				Name:     displayName,
				TestName: testName,
			}
			
			if _, err := os.Stat(sha2File); err == nil {
				// Both files exist, compare them
				identical, err := compareJSONFiles(path, sha2File)
				if err != nil {
					log.Printf("Error comparing files %s and %s: %v", path, sha2File, err)
					comparison.HasDiff = true // Assume different if we can't compare
				} else {
					comparison.Identical = identical
					comparison.HasDiff = !identical
				}
			} else {
				// File only exists in SHA1
				comparison.OnlyInSha1 = true
				comparison.HasDiff = true
			}
			
			fileComparisons[relPath] = comparison
		}
		return nil
	})
	
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Walk through SHA2 files to find files only in SHA2
	err = filepath.Walk(sha2Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".json") {
			relPath, _ := filepath.Rel(sha2Dir, path)
			
			// If we haven't seen this file in SHA1, it's only in SHA2
			if _, exists := fileComparisons[relPath]; !exists {
				baseName := filepath.Base(path)
				dir := filepath.Dir(relPath)
				var testName string
				var displayName string
				
				if dir == "." {
					testName = strings.TrimSuffix(baseName, ".json")
					displayName = baseName
				} else {
					testName = dir
					displayName = fmt.Sprintf("%s (%s)", testName, baseName)
				}
				
				fileComparisons[relPath] = &FileComparison{
					Path:       relPath,
					Name:       displayName,
					TestName:   testName,
					OnlyInSha2: true,
					HasDiff:    true,
				}
			}
		}
		return nil
	})
	
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Convert map to slice for JSON response
	var comparisons []FileComparison
	for _, comp := range fileComparisons {
		comparisons = append(comparisons, *comp)
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(comparisons)
}

func compareJSONFiles(file1, file2 string) (bool, error) {
	// Read both files
	data1, err := os.ReadFile(file1)
	if err != nil {
		return false, err
	}
	
	data2, err := os.ReadFile(file2)
	if err != nil {
		return false, err
	}
	
	// Parse JSON to normalize formatting
	var json1, json2 interface{}
	
	if err := json.Unmarshal(data1, &json1); err != nil {
		return false, err
	}
	
	if err := json.Unmarshal(data2, &json2); err != nil {
		return false, err
	}
	
	// Compare the parsed JSON structures
	normalized1, _ := json.Marshal(json1)
	normalized2, _ := json.Marshal(json2)
	
	return string(normalized1) == string(normalized2), nil
}

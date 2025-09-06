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

type FileInfo struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	TestName string `json:"testName"`
}

func main() {
	var port int
	flag.IntVar(&port, "port", 8080, "Port to serve on")
	flag.Parse()

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

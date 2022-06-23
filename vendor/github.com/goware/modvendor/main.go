package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	zglob "github.com/mattn/go-zglob"
)

var (
	flags       = flag.NewFlagSet("modvendor", flag.ExitOnError)
	copyPatFlag = flags.String("copy", "", "copy files matching glob pattern to ./vendor/ (ie. modvendor -copy=\"**/*.c **/*.h **/*.proto\")")
	verboseFlag = flags.Bool("v", false, "verbose output")
	includeFlag = flags.String(
		"include",
		"",
		`specifies additional directories to copy into ./vendor/ which are not specified in ./vendor/modules.txt. Multiple directories can be included by comma separation e.g. -include:github.com/a/b/dir1,github.com/a/b/dir1/dir2`)
)

type Mod struct {
	ImportPath    string
	SourcePath    string
	Version       string
	SourceVersion string
	Dir           string          // full path, $GOPATH/pkg/mod/
	Pkgs          []string        // sub-pkg import paths
	VendorList    map[string]bool // files to vendor
}

func main() {
	flags.Parse(os.Args[1:])

	// Ensure go.mod file exists and we're running from the project root,
	// and that ./vendor/modules.txt file exists.
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if _, err := os.Stat(filepath.Join(cwd, "go.mod")); os.IsNotExist(err) {
		fmt.Println("Whoops, cannot find `go.mod` file")
		os.Exit(1)
	}
	modtxtPath := filepath.Join(cwd, "vendor", "modules.txt")
	if _, err := os.Stat(modtxtPath); os.IsNotExist(err) {
		fmt.Println("Whoops, cannot find vendor/modules.txt, first run `go mod vendor` and try again")
		os.Exit(1)
	}

	// Prepare vendor copy patterns
	copyPat := strings.Split(strings.TrimSpace(*copyPatFlag), " ")
	if len(copyPat) == 0 {
		fmt.Println("Whoops, -copy argument is empty, nothing to copy.")
		os.Exit(1)
	}
	additionalDirsToInclude := strings.Split(*includeFlag, ",")

	// Parse/process modules.txt file of pkgs
	f, _ := os.Open(modtxtPath)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	var mod *Mod
	modules := []*Mod{}

	for scanner.Scan() {
		line := scanner.Text()

		if line[0] == 35 {
			s := strings.Split(line, " ")
			if (len(s) != 6 && len(s) != 3) || s[1] == "explicit" {
				continue
			}

			mod = &Mod{
				ImportPath: s[1],
				Version:    s[2],
			}
			if s[2] == "=>" {
				// issue https://github.com/golang/go/issues/33848 added these,
				// see comments. I think we can get away with ignoring them.
				continue
			}
			// Handle "replace" in module file if any
			if len(s) > 3 && s[3] == "=>" {
				mod.SourcePath = s[4]

				// Handle replaces with a relative target. For example:
				// "replace github.com/status-im/status-go/protocol => ./protocol"
				if strings.HasPrefix(s[4], ".") || strings.HasPrefix(s[4], "/") {
					mod.Dir, err = filepath.Abs(s[4])
					if err != nil {
						fmt.Printf("invalid relative path: %v", err)
						os.Exit(1)
					}
				} else {
					mod.SourceVersion = s[5]
					mod.Dir = pkgModPath(mod.SourcePath, mod.SourceVersion)
				}
			} else {
				mod.Dir = pkgModPath(mod.ImportPath, mod.Version)
			}

			if _, err := os.Stat(mod.Dir); os.IsNotExist(err) {
				fmt.Printf("Error! %q module path does not exist, check $GOPATH/pkg/mod\n", mod.Dir)
				os.Exit(1)
			}

			// Build list of files to module path source to project vendor folder
			mod.VendorList = buildModVendorList(copyPat, mod)
			// Append directories we need to also include which may not be in vendor/modules.txt.
			for _, dir := range additionalDirsToInclude {
				if strings.HasPrefix(dir, mod.ImportPath) {
					mod.Pkgs = append(mod.Pkgs, dir)
				}
			}

			modules = append(modules, mod)

			continue
		}

		mod.Pkgs = append(mod.Pkgs, line)
	}

	// Filter out files not part of the mod.Pkgs
	for _, mod := range modules {
		if len(mod.VendorList) == 0 {
			continue
		}
		for vendorFile, _ := range mod.VendorList {
			for _, subpkg := range mod.Pkgs {
				path := filepath.Join(mod.Dir, importPathIntersect(mod.ImportPath, subpkg))

				x := strings.Index(vendorFile, path)
				if x == 0 {
					mod.VendorList[vendorFile] = true
				}
			}
		}
		for vendorFile, toggle := range mod.VendorList {
			if !toggle {
				delete(mod.VendorList, vendorFile)
			}
		}
	}

	// Copy mod vendor list files to ./vendor/
	for _, mod := range modules {
		for vendorFile := range mod.VendorList {
			x := strings.Index(vendorFile, mod.Dir)
			if x < 0 {
				fmt.Println("Error! vendor file doesn't belong to mod, strange.")
				os.Exit(1)
			}

			localPath := fmt.Sprintf("%s%s", mod.ImportPath, vendorFile[len(mod.Dir):])
			localFile := fmt.Sprintf("./vendor/%s", localPath)

			if *verboseFlag {
				fmt.Printf("vendoring %s\n", localPath)
			}

			os.MkdirAll(filepath.Dir(localFile), os.ModePerm)
			if _, err := copyFile(vendorFile, localFile); err != nil {
				fmt.Printf("Error! %s - unable to copy file %s\n", err.Error(), vendorFile)
				os.Exit(1)
			}
		}
	}
}

func buildModVendorList(copyPat []string, mod *Mod) map[string]bool {
	vendorList := map[string]bool{}

	for _, pat := range copyPat {
		matches, err := zglob.Glob(filepath.Join(mod.Dir, pat))
		if err != nil {
			fmt.Println("Error! glob match failure:", err)
			os.Exit(1)
		}

		for _, m := range matches {
			vendorList[m] = false
		}
	}

	return vendorList
}

func importPathIntersect(basePath, pkgPath string) string {
	if strings.Index(pkgPath, basePath) != 0 {
		return ""
	}
	return pkgPath[len(basePath):]
}

func normString(str string) (normStr string) {
	for _, char := range str {
		if unicode.IsUpper(char) {
			normStr += "!" + string(unicode.ToLower(char))
		} else {
			normStr += string(char)
		}
	}
	return
}

func pkgModPath(importPath, version string) string {
	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		// the default GOPATH for go v1.11
		goPath = filepath.Join(os.Getenv("HOME"), "go")
	}

	normPath := normString(importPath)
	normVersion := normString(version)

	return filepath.Join(goPath, "pkg", "mod", fmt.Sprintf("%s@%s", normPath, normVersion))
}

func copyFile(src, dst string) (int64, error) {
	srcStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !srcStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	return io.Copy(dstFile, srcFile)
}

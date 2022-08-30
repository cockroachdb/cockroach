package imports

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/gostdlib/x/tools/internal/gopathwalk"
	"github.com/cockroachdb/gostdlib/x/tools/internal/module"
	"github.com/cockroachdb/gostdlib/x/tools/internal/semver"
)

// ModuleResolver implements resolver for modules using the go command as little
// as feasible.
type ModuleResolver struct {
	env            *ProcessEnv
	moduleCacheDir string
	dummyVendorMod *ModuleJSON // If vendoring is enabled, the pseudo-module that represents the /vendor directory.

	Initialized   bool
	Main          *ModuleJSON
	ModsByModPath []*ModuleJSON // All modules, ordered by # of path components in module Path...
	ModsByDir     []*ModuleJSON // ...or Dir.

	// moduleCacheCache stores information about the module cache.
	moduleCacheCache *dirInfoCache
	otherCache       *dirInfoCache
}

type ModuleJSON struct {
	Path      string      // module path
	Replace   *ModuleJSON // replaced by this module
	Main      bool        // is this the main module?
	Dir       string      // directory holding files for this module, if any
	GoMod     string      // path to go.mod file for this module, if any
	GoVersion string      // go version used in module
}

func (r *ModuleResolver) init() error {
	if r.Initialized {
		return nil
	}
	mainMod, vendorEnabled, err := vendorEnabled(r.env)
	if err != nil {
		return err
	}

	if mainMod != nil && vendorEnabled {
		// Vendor mode is on, so all the non-Main modules are irrelevant,
		// and we need to search /vendor for everything.
		r.Main = mainMod
		r.dummyVendorMod = &ModuleJSON{
			Path: "",
			Dir:  filepath.Join(mainMod.Dir, "vendor"),
		}
		r.ModsByModPath = []*ModuleJSON{mainMod, r.dummyVendorMod}
		r.ModsByDir = []*ModuleJSON{mainMod, r.dummyVendorMod}
	} else {
		// Vendor mode is off, so run go list -m ... to find everything.
		r.initAllMods()
	}

	r.moduleCacheDir = filepath.Join(filepath.SplitList(r.env.GOPATH)[0], "/pkg/mod")

	sort.Slice(r.ModsByModPath, func(i, j int) bool {
		count := func(x int) int {
			return strings.Count(r.ModsByModPath[x].Path, "/")
		}
		return count(j) < count(i) // descending order
	})
	sort.Slice(r.ModsByDir, func(i, j int) bool {
		count := func(x int) int {
			return strings.Count(r.ModsByDir[x].Dir, "/")
		}
		return count(j) < count(i) // descending order
	})

	if r.moduleCacheCache == nil {
		r.moduleCacheCache = &dirInfoCache{
			dirs: map[string]*directoryPackageInfo{},
		}
	}
	if r.otherCache == nil {
		r.otherCache = &dirInfoCache{
			dirs: map[string]*directoryPackageInfo{},
		}
	}
	r.Initialized = true
	return nil
}

func (r *ModuleResolver) initAllMods() error {
	stdout, err := r.env.invokeGo("list", "-m", "-json", "...")
	if err != nil {
		return err
	}
	for dec := json.NewDecoder(stdout); dec.More(); {
		mod := &ModuleJSON{}
		if err := dec.Decode(mod); err != nil {
			return err
		}
		if mod.Dir == "" {
			if r.env.Debug {
				r.env.Logf("module %v has not been downloaded and will be ignored", mod.Path)
			}
			// Can't do anything with a module that's not downloaded.
			continue
		}
		r.ModsByModPath = append(r.ModsByModPath, mod)
		r.ModsByDir = append(r.ModsByDir, mod)
		if mod.Main {
			r.Main = mod
		}
	}
	return nil
}

func (r *ModuleResolver) ClearForNewScan() {
	r.otherCache = &dirInfoCache{
		dirs: map[string]*directoryPackageInfo{},
	}
}

func (r *ModuleResolver) ClearForNewMod() {
	env := r.env
	*r = ModuleResolver{
		env: env,
	}
	r.init()
}

// findPackage returns the module and directory that contains the package at
// the given import path, or returns nil, "" if no module is in scope.
func (r *ModuleResolver) findPackage(importPath string) (*ModuleJSON, string) {
	// This can't find packages in the stdlib, but that's harmless for all
	// the existing code paths.
	for _, m := range r.ModsByModPath {
		if !strings.HasPrefix(importPath, m.Path) {
			continue
		}
		pathInModule := importPath[len(m.Path):]
		pkgDir := filepath.Join(m.Dir, pathInModule)
		if r.dirIsNestedModule(pkgDir, m) {
			continue
		}

		if info, ok := r.cacheLoad(pkgDir); ok {
			if loaded, err := info.reachedStatus(nameLoaded); loaded {
				if err != nil {
					continue // No package in this dir.
				}
				return m, pkgDir
			}
			if scanned, err := info.reachedStatus(directoryScanned); scanned && err != nil {
				continue // Dir is unreadable, etc.
			}
			// This is slightly wrong: a directory doesn't have to have an
			// importable package to count as a package for package-to-module
			// resolution. package main or _test files should count but
			// don't.
			// TODO(heschi): fix this.
			if _, err := r.cachePackageName(pkgDir); err == nil {
				return m, pkgDir
			}
		}

		// Not cached. Read the filesystem.
		pkgFiles, err := ioutil.ReadDir(pkgDir)
		if err != nil {
			continue
		}
		// A module only contains a package if it has buildable go
		// files in that directory. If not, it could be provided by an
		// outer module. See #29736.
		for _, fi := range pkgFiles {
			if ok, _ := r.env.buildContext().MatchFile(pkgDir, fi.Name()); ok {
				return m, pkgDir
			}
		}
	}
	return nil, ""
}

func (r *ModuleResolver) cacheLoad(dir string) (directoryPackageInfo, bool) {
	if info, ok := r.moduleCacheCache.Load(dir); ok {
		return info, ok
	}
	return r.otherCache.Load(dir)
}

func (r *ModuleResolver) cacheStore(info directoryPackageInfo) {
	if info.rootType == gopathwalk.RootModuleCache {
		r.moduleCacheCache.Store(info.dir, info)
	} else {
		r.otherCache.Store(info.dir, info)
	}
}

func (r *ModuleResolver) cacheKeys() []string {
	return append(r.moduleCacheCache.Keys(), r.otherCache.Keys()...)
}

// cachePackageName caches the package name for a dir already in the cache.
func (r *ModuleResolver) cachePackageName(dir string) (directoryPackageInfo, error) {
	info, ok := r.cacheLoad(dir)
	if !ok {
		panic("cachePackageName on uncached dir " + dir)
	}

	loaded, err := info.reachedStatus(nameLoaded)
	if loaded {
		return info, err
	}
	info.packageName, info.err = packageDirToName(info.dir)
	info.status = nameLoaded
	r.cacheStore(info)
	return info, info.err
}

// findModuleByDir returns the module that contains dir, or nil if no such
// module is in scope.
func (r *ModuleResolver) findModuleByDir(dir string) *ModuleJSON {
	// This is quite tricky and may not be correct. dir could be:
	// - a package in the main module.
	// - a replace target underneath the main module's directory.
	//    - a nested module in the above.
	// - a replace target somewhere totally random.
	//    - a nested module in the above.
	// - in the mod cache.
	// - in /vendor/ in -mod=vendor mode.
	//    - nested module? Dunno.
	// Rumor has it that replace targets cannot contain other replace targets.
	for _, m := range r.ModsByDir {
		if !strings.HasPrefix(dir, m.Dir) {
			continue
		}

		if r.dirIsNestedModule(dir, m) {
			continue
		}

		return m
	}
	return nil
}

// dirIsNestedModule reports if dir is contained in a nested module underneath
// mod, not actually in mod.
func (r *ModuleResolver) dirIsNestedModule(dir string, mod *ModuleJSON) bool {
	if !strings.HasPrefix(dir, mod.Dir) {
		return false
	}
	if r.dirInModuleCache(dir) {
		// Nested modules in the module cache are pruned,
		// so it cannot be a nested module.
		return false
	}
	if mod != nil && mod == r.dummyVendorMod {
		// The /vendor pseudomodule is flattened and doesn't actually count.
		return false
	}
	modDir, _ := r.modInfo(dir)
	if modDir == "" {
		return false
	}
	return modDir != mod.Dir
}

func (r *ModuleResolver) modInfo(dir string) (modDir string, modName string) {
	readModName := func(modFile string) string {
		modBytes, err := ioutil.ReadFile(modFile)
		if err != nil {
			return ""
		}
		return modulePath(modBytes)
	}

	if r.dirInModuleCache(dir) {
		matches := modCacheRegexp.FindStringSubmatch(dir)
		index := strings.Index(dir, matches[1]+"@"+matches[2])
		modDir := filepath.Join(dir[:index], matches[1]+"@"+matches[2])
		return modDir, readModName(filepath.Join(modDir, "go.mod"))
	}
	for {
		if info, ok := r.cacheLoad(dir); ok {
			return info.moduleDir, info.moduleName
		}
		f := filepath.Join(dir, "go.mod")
		info, err := os.Stat(f)
		if err == nil && !info.IsDir() {
			return dir, readModName(f)
		}

		d := filepath.Dir(dir)
		if len(d) >= len(dir) {
			return "", "" // reached top of file system, no go.mod
		}
		dir = d
	}
}

func (r *ModuleResolver) dirInModuleCache(dir string) bool {
	if r.moduleCacheDir == "" {
		return false
	}
	return strings.HasPrefix(dir, r.moduleCacheDir)
}

func (r *ModuleResolver) loadPackageNames(importPaths []string, srcDir string) (map[string]string, error) {
	if err := r.init(); err != nil {
		return nil, err
	}
	names := map[string]string{}
	for _, path := range importPaths {
		_, packageDir := r.findPackage(path)
		if packageDir == "" {
			continue
		}
		name, err := packageDirToName(packageDir)
		if err != nil {
			continue
		}
		names[path] = name
	}
	return names, nil
}

func (r *ModuleResolver) scan(_ references, loadNames bool, exclude []gopathwalk.RootType) ([]*pkg, error) {
	if err := r.init(); err != nil {
		return nil, err
	}

	// Walk GOROOT, GOPATH/pkg/mod, and the main module.
	roots := []gopathwalk.Root{
		{filepath.Join(r.env.GOROOT, "/src"), gopathwalk.RootGOROOT},
	}
	if r.Main != nil {
		roots = append(roots, gopathwalk.Root{r.Main.Dir, gopathwalk.RootCurrentModule})
	}
	if r.dummyVendorMod != nil {
		roots = append(roots, gopathwalk.Root{r.dummyVendorMod.Dir, gopathwalk.RootOther})
	} else {
		roots = append(roots, gopathwalk.Root{r.moduleCacheDir, gopathwalk.RootModuleCache})
		// Walk replace targets, just in case they're not in any of the above.
		for _, mod := range r.ModsByModPath {
			if mod.Replace != nil {
				roots = append(roots, gopathwalk.Root{mod.Dir, gopathwalk.RootOther})
			}
		}
	}

	roots = filterRoots(roots, exclude)

	var result []*pkg
	var mu sync.Mutex

	// We assume cached directories have not changed. We can skip them and their
	// children.
	skip := func(root gopathwalk.Root, dir string) bool {
		mu.Lock()
		defer mu.Unlock()

		info, ok := r.cacheLoad(dir)
		if !ok {
			return false
		}
		// This directory can be skipped as long as we have already scanned it.
		// Packages with errors will continue to have errors, so there is no need
		// to rescan them.
		packageScanned, _ := info.reachedStatus(directoryScanned)
		return packageScanned
	}

	// Add anything new to the cache. We'll process everything in it below.
	add := func(root gopathwalk.Root, dir string) {
		mu.Lock()
		defer mu.Unlock()

		r.cacheStore(r.scanDirForPackage(root, dir))
	}

	gopathwalk.WalkSkip(roots, add, skip, gopathwalk.Options{Debug: r.env.Debug, ModulesEnabled: true})

	// Everything we already had, and everything new, is now in the cache.
	for _, dir := range r.cacheKeys() {
		info, ok := r.cacheLoad(dir)
		if !ok {
			continue
		}

		// Skip this directory if we were not able to get the package information successfully.
		if scanned, err := info.reachedStatus(directoryScanned); !scanned || err != nil {
			continue
		}

		// If we want package names, make sure the cache has them.
		if loadNames {
			var err error
			if info, err = r.cachePackageName(info.dir); err != nil {
				continue
			}
		}

		res, err := r.canonicalize(info)
		if err != nil {
			continue
		}
		result = append(result, res)
	}

	return result, nil
}

// canonicalize gets the result of canonicalizing the packages using the results
// of initializing the resolver from 'go list -m'.
func (r *ModuleResolver) canonicalize(info directoryPackageInfo) (*pkg, error) {
	// Packages in GOROOT are already canonical, regardless of the std/cmd modules.
	if info.rootType == gopathwalk.RootGOROOT {
		return &pkg{
			importPathShort: info.nonCanonicalImportPath,
			dir:             info.dir,
			packageName:     path.Base(info.nonCanonicalImportPath),
		}, nil
	}

	importPath := info.nonCanonicalImportPath
	// Check if the directory is underneath a module that's in scope.
	if mod := r.findModuleByDir(info.dir); mod != nil {
		// It is. If dir is the target of a replace directive,
		// our guessed import path is wrong. Use the real one.
		if mod.Dir == info.dir {
			importPath = mod.Path
		} else {
			dirInMod := info.dir[len(mod.Dir)+len("/"):]
			importPath = path.Join(mod.Path, filepath.ToSlash(dirInMod))
		}
	} else if info.needsReplace {
		return nil, fmt.Errorf("package in %q is not valid without a replace statement", info.dir)
	}

	res := &pkg{
		importPathShort: importPath,
		dir:             info.dir,
		packageName:     info.packageName, // may not be populated if the caller didn't ask for it
	}
	// We may have discovered a package that has a different version
	// in scope already. Canonicalize to that one if possible.
	if _, canonicalDir := r.findPackage(importPath); canonicalDir != "" {
		res.dir = canonicalDir
	}
	return res, nil
}

func (r *ModuleResolver) loadExports(ctx context.Context, expectPackage string, pkg *pkg) (map[string]bool, error) {
	if err := r.init(); err != nil {
		return nil, err
	}
	return loadExportsFromFiles(ctx, r.env, expectPackage, pkg.dir)
}

func (r *ModuleResolver) scanDirForPackage(root gopathwalk.Root, dir string) directoryPackageInfo {
	subdir := ""
	if dir != root.Path {
		subdir = dir[len(root.Path)+len("/"):]
	}
	importPath := filepath.ToSlash(subdir)
	if strings.HasPrefix(importPath, "vendor/") {
		// Only enter vendor directories if they're explicitly requested as a root.
		return directoryPackageInfo{
			status: directoryScanned,
			err:    fmt.Errorf("unwanted vendor directory"),
		}
	}
	switch root.Type {
	case gopathwalk.RootCurrentModule:
		importPath = path.Join(r.Main.Path, filepath.ToSlash(subdir))
	case gopathwalk.RootModuleCache:
		matches := modCacheRegexp.FindStringSubmatch(subdir)
		if len(matches) == 0 {
			return directoryPackageInfo{
				status: directoryScanned,
				err:    fmt.Errorf("invalid module cache path: %v", subdir),
			}
		}
		modPath, err := module.DecodePath(filepath.ToSlash(matches[1]))
		if err != nil {
			if r.env.Debug {
				r.env.Logf("decoding module cache path %q: %v", subdir, err)
			}
			return directoryPackageInfo{
				status: directoryScanned,
				err:    fmt.Errorf("decoding module cache path %q: %v", subdir, err),
			}
		}
		importPath = path.Join(modPath, filepath.ToSlash(matches[3]))
	}

	modDir, modName := r.modInfo(dir)
	result := directoryPackageInfo{
		status:                 directoryScanned,
		dir:                    dir,
		rootType:               root.Type,
		nonCanonicalImportPath: importPath,
		needsReplace:           false,
		moduleDir:              modDir,
		moduleName:             modName,
	}
	if root.Type == gopathwalk.RootGOROOT {
		// stdlib packages are always in scope, despite the confusing go.mod
		return result
	}
	// Check that this package is not obviously impossible to import.
	if !strings.HasPrefix(importPath, modName) {
		// The module's declared path does not match
		// its expected path. It probably needs a
		// replace directive we don't have.
		result.needsReplace = true
	}

	return result
}

// modCacheRegexp splits a path in a module cache into module, module version, and package.
var modCacheRegexp = regexp.MustCompile(`(.*)@([^/\\]*)(.*)`)

var (
	slashSlash = []byte("//")
	moduleStr  = []byte("module")
)

// modulePath returns the module path from the gomod file text.
// If it cannot find a module path, it returns an empty string.
// It is tolerant of unrelated problems in the go.mod file.
//
// Copied from cmd/go/internal/modfile.
func modulePath(mod []byte) string {
	for len(mod) > 0 {
		line := mod
		mod = nil
		if i := bytes.IndexByte(line, '\n'); i >= 0 {
			line, mod = line[:i], line[i+1:]
		}
		if i := bytes.Index(line, slashSlash); i >= 0 {
			line = line[:i]
		}
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, moduleStr) {
			continue
		}
		line = line[len(moduleStr):]
		n := len(line)
		line = bytes.TrimSpace(line)
		if len(line) == n || len(line) == 0 {
			continue
		}

		if line[0] == '"' || line[0] == '`' {
			p, err := strconv.Unquote(string(line))
			if err != nil {
				return "" // malformed quoted string or multiline module path
			}
			return p
		}

		return string(line)
	}
	return "" // missing module path
}

var modFlagRegexp = regexp.MustCompile(`-mod[ =](\w+)`)

// vendorEnabled indicates if vendoring is enabled.
// Inspired by setDefaultBuildMod in modload/init.go
func vendorEnabled(env *ProcessEnv) (*ModuleJSON, bool, error) {
	mainMod, go114, err := getMainModuleAnd114(env)
	if err != nil {
		return nil, false, err
	}
	matches := modFlagRegexp.FindStringSubmatch(env.GOFLAGS)
	var modFlag string
	if len(matches) != 0 {
		modFlag = matches[1]
	}
	if modFlag != "" {
		// Don't override an explicit '-mod=' argument.
		return mainMod, modFlag == "vendor", nil
	}
	if mainMod == nil || !go114 {
		return mainMod, false, nil
	}
	// Check 1.14's automatic vendor mode.
	if fi, err := os.Stat(filepath.Join(mainMod.Dir, "vendor")); err == nil && fi.IsDir() {
		if mainMod.GoVersion != "" && semver.Compare("v"+mainMod.GoVersion, "v1.14") >= 0 {
			// The Go version is at least 1.14, and a vendor directory exists.
			// Set -mod=vendor by default.
			return mainMod, true, nil
		}
	}
	return mainMod, false, nil
}

// getMainModuleAnd114 gets the main module's information and whether the
// go command in use is 1.14+. This is the information needed to figure out
// if vendoring should be enabled.
func getMainModuleAnd114(env *ProcessEnv) (*ModuleJSON, bool, error) {
	const format = `{{.Path}}
{{.Dir}}
{{.GoMod}}
{{.GoVersion}}
{{range context.ReleaseTags}}{{if eq . "go1.14"}}{{.}}{{end}}{{end}}
`
	stdout, err := env.invokeGo("list", "-m", "-f", format)
	if err != nil {
		return nil, false, nil
	}
	lines := strings.Split(stdout.String(), "\n")
	if len(lines) < 5 {
		return nil, false, fmt.Errorf("unexpected stdout: %q", stdout)
	}
	mod := &ModuleJSON{
		Path:      lines[0],
		Dir:       lines[1],
		GoMod:     lines[2],
		GoVersion: lines[3],
		Main:      true,
	}
	return mod, lines[4] == "go1.14", nil
}

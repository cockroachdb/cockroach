// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package urlcheck

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func checkGithubRateLimit(client *http.Client) (limit int, untilReset time.Duration, err error) {
	resp, err := client.Get("https://api.github.com/rate_limit")
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("unexpected status: %s", resp.Status)
	}
	limit, err = strconv.Atoi(resp.Header.Get("X-RateLimit-Remaining"))
	if err != nil {
		return 0, 0, err
	}
	sec, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return 0, 0, err
	}
	reset := timeutil.Unix(sec, 0)
	if !reset.After(timeutil.Now()) {
		return limit, 0, nil
	}
	return limit, timeutil.Until(reset), nil
}

func existsGitHubSitePath(client *http.Client, u *url.URL) error {
	parts := splitPath(u.Path)
	if len(parts) == 0 {
		// Trivially, the root URL exists.
		return nil
	}

	// 1) /<login>  → user/org exists
	if len(parts) == 1 {
		api := fmt.Sprintf("https://api.github.com/users/%s", parts[0])
		return restExists(client, api)
	}

	owner, repo := parts[0], parts[1]

	// 2) /<owner>/<repo> → repo exists
	if len(parts) == 2 {
		return repoExists(client, owner, repo)
	}

	// Normalize a couple of common typos/variants:
	// - /pulls/<n> → /pull/<n>
	// - /issue/<n> → /issues/<n>
	isNumeric := func(s string) bool {
		for _, r := range s {
			if r < '0' || r > '9' {
				return false
			}
		}
		return s != ""
	}
	if parts[2] == "pulls" && len(parts) >= 4 && isNumeric(parts[3]) {
		parts[2] = "pull"
	}
	if parts[2] == "issue" && len(parts) >= 4 && isNumeric(parts[3]) {
		parts[2] = "issues"
	}

	switch parts[2] {

	// --- Wiki pages ---------------------------------------------------------
	case "wiki":
		// /<owner>/<repo>/wiki[/<Page...>[/<rev40>]]
		// 1) root wiki → Home.*
		if len(parts) == 3 || (len(parts) == 4 && parts[3] == "") {
			if ok, err := existsWikiRaw(client, owner, repo, []string{}); err != nil {
				return err
			} else if !ok {
				return errors.New("wiki page not found")
			}
			// found
			return nil
		}

		// 2) page (optionally with a trailing 40-hex revision)
		pageParts := parts[3:]
		var rev string
		if n := len(pageParts); n >= 1 && isHex40(pageParts[n-1]) {
			rev = pageParts[n-1]
			pageParts = pageParts[:n-1]
		}
		// Resolve the page via raw wiki (no API for wikis)
		ok, err := existsWikiRaw(client, owner, repo, pageParts)
		if err != nil || !ok {
			return err
		}
		if rev == "" {
			return nil
		}
		// If a specific revision was requested, HEAD the exact HTML URL as a last resort.
		_, err = head200(client, u.String())
		return err

	// --- Releases & assets --------------------------------------------------
	case "releases":
		// /releases (list view) → treat as exists if repo exists
		if len(parts) == 3 || (len(parts) == 4 && parts[3] == "") {
			return repoExists(client, owner, repo)
		}
		// /releases/tag/<tag>
		if len(parts) >= 5 && parts[3] == "tag" {
			tag := parts[4]
			return releaseTagExists(client, owner, repo, tag)
		}
		// /releases/download/<tag>/<name>
		if len(parts) >= 6 && parts[3] == "download" {
			tag := parts[4]
			name := strings.Join(parts[5:], "/") // asset filename may contain slashes
			return releaseAssetExistsByTag(client, owner, repo, tag, name)
		}
		// Fallback: repo exists
		return repoExists(client, owner, repo)

	// --- Archives -----------------------------------------------------------
	case "archive":
		// Supports:
		//   /archive/<ref>.zip|tar.gz
		//   /archive/refs/tags/<tag>.zip|tar.gz
		var refWithExt string
		if len(parts) >= 6 && parts[3] == "refs" && parts[4] == "tags" {
			refWithExt = parts[5]
		} else if len(parts) >= 4 {
			refWithExt = parts[3]
		} else {
			return repoExists(client, owner, repo)
		}
		ref := strings.TrimSuffix(strings.TrimSuffix(refWithExt, ".zip"), ".tar.gz")

		// Try commit SHA, then heads/tags.
		if err := restExists(client,
			fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", owner, repo, url.PathEscape(ref))); err == nil {
			return nil
		}
		return gitRefExists(client, owner, repo, ref)

	// --- PRs ----------------------------------------------------------------
	case "pull":
		if len(parts) >= 4 && isNumeric(parts[3]) {
			return restExists(client,
				fmt.Sprintf("https://api.github.com/repos/%s/%s/pulls/%s", owner, repo, parts[3]))
		}
		// /pull (unexpected) → list view semantics
		return repoExists(client, owner, repo)
	case "pulls":
		// List/search view → exists if repo exists
		return repoExists(client, owner, repo)

	// --- Issues -------------------------------------------------------------
	case "issues":
		// /issues/<n> or list/search view (/issues or /issues?q=...)
		if len(parts) >= 4 && isNumeric(parts[3]) {
			return restExists(client,
				fmt.Sprintf("https://api.github.com/repos/%s/%s/issues/%s", owner, repo, parts[3]))
		}
		return repoExists(client, owner, repo)

		// --- Commits (list view) and single commit --------------------------------
	case "commits":
		// /<owner>/<repo>/commits[/<ref>[/<path...>]]
		// The web UI lets <ref> contain slashes and (optionally) a trailing path filter.
		// We greedily match the longest existing ref; the path (if any) is ignored for existence.
		if len(parts) == 3 {
			// /commits → list view always exists if repo exists
			return repoExists(client, owner, repo)
		}
		// Try to find the longest ref prefix from parts[3:].
		if _, ok, err := findExistingRefPrefix(client, owner, repo, parts[3:]); err != nil {
			return err
		} else if ok {
			return nil // ref exists → the commits page exists
		}
		// If there’s only a query (e.g., /commits?author=...), treat as list view.
		if len(parts) == 3 && u.RawQuery != "" {
			return repoExists(client, owner, repo)
		}
		return nil

	case "commit":
		if len(parts) >= 4 {
			return restExists(client,
				fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", owner, repo, parts[3]))
		}
		return repoExists(client, owner, repo)

	// --- Releases by tag were handled above; Labels -------------------------
	case "labels":
		// If you care about label *existence*:
		if len(parts) >= 4 {
			lbl, _ := url.PathUnescape(strings.Join(parts[3:], "/"))
			return restExists(client,
				fmt.Sprintf("https://api.github.com/repos/%s/%s/labels/%s", owner, repo, url.PathEscape(lbl)))
		}
		// Otherwise, page exists if repo exists:
		return repoExists(client, owner, repo)

	// --- Actions tab --------------------------------------------------------
	case "actions":
		// UI tab → consider it existing if repo exists (optional: list workflows)
		return repoExists(client, owner, repo)

	// --- Blob/Tree (file/directory at ref) ---------------------------------
	case "blob", "tree":
		// /<owner>/<repo>/(blob|tree)/<ref>/<path...>
		// If only /tree/<ref>, verify ref exists. If a path is present, use Contents API.
		if len(parts) == 4 && parts[2] == "tree" {
			ref := parts[3]
			return gitRefExists(client, owner, repo, ref)
		}
		if len(parts) >= 5 {
			ref := parts[3]
			pth := escapePathSegments(parts[4:])
			api := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s?ref=%s",
				owner, repo, pth, url.QueryEscape(ref))
			return restExists(client, api)
		}
		return repoExists(client, owner, repo)

	// --- Attachments served under /assets/... (no public REST) --------------
	case "assets":
		return checkURL(client, u.String())

	default:
		// Many other tab/list/search views (e.g., /releases already handled, /contributors, /network, etc.)
		// Treat as existing if the repo exists.
		return repoExists(client, owner, repo)
	}
}

func existsRawContent(client *http.Client, u *url.URL) error {
	// raw.githubusercontent.com/<owner>/<repo>/<ref>/<path...>
	parts := splitPath(u.Path)
	if len(parts) < 4 {
		return fmt.Errorf("unsupported raw URL: %s", u)
	}
	owner, repo, ref := parts[0], parts[1], parts[2]
	pth := escapePathSegments(parts[3:])
	api := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s?ref=%s", owner, repo, pth, url.QueryEscape(ref))
	return restExists(client, api)
}

func existsGist(client *http.Client, u *url.URL) error {
	// gist.github.com/{user?}/{gist_id}[/{sha}]
	parts := splitPath(u.Path)
	var id, sha string
	switch len(parts) {
	case 1:
		id = parts[0]
	case 2:
		id = parts[1]
	default:
		// /{user}/{id}/{sha}/...
		id = parts[1]
		sha = parts[2]
	}
	api := "https://api.github.com/gists/" + id
	if sha != "" {
		api += "/" + sha // specific revision
	}
	return restExists(client, api)
}

func restExists(client *http.Client, urlStr string) error {
	resp, err := client.Get(urlStr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNotModified:
		return nil
	case http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, urlStr)
	}
}

func repoExists(client *http.Client, owner, repo string) error {
	return restExists(client,
		fmt.Sprintf("https://api.github.com/repos/%s/%s", owner, repo))
}

func releaseTagExists(client *http.Client, owner, repo, tag string) error {
	return restExists(client,
		fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/tags/%s",
			owner, repo, url.PathEscape(tag)))
}

func releaseAssetExistsByTag(client *http.Client, owner, repo, tag, name string) error {
	// Fetch release by tag and scan assets for matching name
	api := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/tags/%s",
		owner, repo, url.PathEscape(tag))
	resp, err := client.Get(api)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected %d", resp.StatusCode)
	}
	var r struct {
		Assets []struct{ Name string } `json:"assets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return err
	}
	for _, a := range r.Assets {
		if a.Name == name {
			return nil
		}
	}
	// If the release exists but asset name doesn't match exactly, treat as not found.
	return errors.New("asset not found in release")
}

// Greedily try parts[3:], parts[3:len-1], ... to find the longest ref that exists
func findExistingRefPrefix(
	client *http.Client, owner, repo string, segs []string,
) (string, bool, error) {
	for i := len(segs); i >= 1; i-- {
		candidate := strings.Join(segs[:i], "/") // allow slashes in branch names
		if err := gitRefExists(client, owner, repo, candidate); err != nil {
			continue
		}
		return candidate, true, nil
	}
	return "", false, nil
}

func gitRefExists(client *http.Client, owner, repo, ref string) error {
	// Try heads/<branch> first, then tags/<tag>
	if err := restExists(client,
		fmt.Sprintf("https://api.github.com/repos/%s/%s/git/ref/heads/%s", owner, repo, url.PathEscape(ref))); err == nil {
		return nil
	}
	if err := restExists(client,
		fmt.Sprintf("https://api.github.com/repos/%s/%s/git/ref/tags/%s", owner, repo, url.PathEscape(ref))); err == nil {
		return nil
	}
	// Fallback to matching-refs (prefix search) for branches and tags
	for _, kind := range []string{"heads", "tags"} {
		api := fmt.Sprintf("https://api.github.com/repos/%s/%s/git/matching-refs/%s/%s",
			owner, repo, kind, url.PathEscape(ref))
		resp, err := client.Get(api)
		if err != nil {
			return err
		}
		func() { defer resp.Body.Close() }()
		if resp.StatusCode == 200 {
			var arr []any
			if json.NewDecoder(resp.Body).Decode(&arr) == nil && len(arr) > 0 {
				return nil
			}
		} else if resp.StatusCode != 404 {
			return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, api)
		}
	}
	return nil
}

// existsWikiRaw probes the *current* wiki page via the raw wiki host.
// pageParts may be nil/empty → Home.*
func existsWikiRaw(client *http.Client, owner, repo string, pageParts []string) (bool, error) {
	page := "Home"
	if len(pageParts) > 0 {
		page = strings.Join(pageParts, "/")
	}
	// Try common extensions (exact case "Home" is important).
	exts := []string{".md", ".markdown", ".rst", ".adoc"}
	for _, ext := range exts {
		raw := fmt.Sprintf("https://raw.githubusercontent.com/wiki/%s/%s/%s%s",
			owner, repo, url.PathEscape(page), ext)
		ok, err := head200(client, raw)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func isHex40(s string) bool {
	if len(s) != 40 {
		return false
	}
	for i := 0; i < 40; i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

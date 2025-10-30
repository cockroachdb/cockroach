// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package future

import (
	"embed"
	"html/template"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

//go:embed assets/*
var assetsFS embed.FS

func MakeFutureHandler() http.HandlerFunc {
	mux := http.ServeMux{}

	// Serve the inline template for both root and /index.html
	serveTemplate := func(w http.ResponseWriter, r *http.Request) {
		tpl, err := template.New("index").Parse(`
<!DOCTYPE html>
<html lang="en">
  <head>
    <script src="assets/htmx.min.js"></script>
    <script defer src="assets/alpine.min.js"></script>
    <script src="assets/auto-refresh.js"></script>
    <link rel=stylesheet href="assets/missing.css">
  </head>

  <body>
  <div class="fullscreen container center align-content:center flex-switch">
    <div>
      <h3>Log in to the DB Console</h3>
      <form method="POST" class="flex-column">
        <div>
          <label for="username">Username:</label>
          <input type="text" id="username"></input>          
        </div>
        <div>
          <label for="password">Password:</label>
          <input type="password" id="password"></input>
        </div>
        <div>
          <strong>
            <button type="submit">Log In</button>
          </strong>
        </div>
      </form>
    </div>
    <div>
      <h4>A user with a password is required to log in to the DB Console on secure clusters.</h4>
      <p>
        Create a user with this SQL command:
        <code class="language-sql">
          CREATE USER craig WITH PASSWORD 'cockroach';
        </code>
      </p>
      <p>
        <a href="#">Read more about configuring login</a>
      </p>
    </div>
  </div>
  </body>
</html>
			`)
		if err != nil {
			http.Error(w, "bad template", 500)
			return
		}
		err = tpl.Execute(w, nil)
		if err != nil {
			http.Error(w, "bad template", 500)
			return
		}
	}

	//mux.HandleFunc("/", serveTemplate)
	mux.HandleFunc("/future/index.html", serveTemplate)
	mux.HandleFunc("/future/assets/", HandleAssets)

	return mux.ServeHTTP
}

func HandleAssets(w http.ResponseWriter, r *http.Request) {
	log.Dev.Warningf(r.Context(), "REQUESTING: %s", r.URL.Path)

	// Get the requested path and strip the /future/assets/ prefix
	requestPath := r.URL.Path
	requestPath = strings.TrimPrefix(requestPath, "/future/assets/")

	// Construct the path within the embedded filesystem
	embedPath := "assets/" + requestPath

	log.Dev.Warningf(r.Context(), "SERVING embedded: %s", embedPath)

	// Try to read the file from the embedded filesystem
	data, err := assetsFS.ReadFile(embedPath)
	if err != nil {
		if strings.Contains(err.Error(), "file does not exist") {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Set no-cache headers for development hot reloading
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	// Detect and set content type based on file extension
	if strings.HasSuffix(requestPath, ".js") {
		w.Header().Set("Content-Type", "application/javascript")
	} else if strings.HasSuffix(requestPath, ".css") {
		w.Header().Set("Content-Type", "text/css")
	} else if strings.HasSuffix(requestPath, ".html") {
		w.Header().Set("Content-Type", "text/html")
	}

	// Write the file contents
	_, _ = w.Write(data)
}

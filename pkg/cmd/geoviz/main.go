// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/build"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geos"
)

var (
	// APIKey is the API key to the Google Maps API.
	APIKey string
)

func init() {
	APIKey = os.Getenv("GEOVIZ_GOOGLE_MAPS_API_KEY")
}

type indexTemplate struct {
	APIKey string
}

// handleIndex serves the HTML page that contains the map.
func handleIndex(w http.ResponseWriter, r *http.Request) {
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	templates := template.Must(template.ParseFiles(filepath.Join(pkg.Dir, "pkg/cmd/geoviz/templates/index.tmpl.html")))
	if err := templates.ExecuteTemplate(
		w,
		"index.tmpl.html",
		indexTemplate{
			APIKey: APIKey,
		},
	); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleLoad parses in the CSV format for displaying geospatial data
// and transforms it into an arrangement that is suitable for display
// using the Google Maps Javascript API.
func handleLoad(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	gviz, err := ImageFromReader(strings.NewReader(r.Form["data"][0]))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ret, err := json.Marshal(gviz)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(ret); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

var flagGeoLibsDir = flag.String(
	"geo_libs",
	"/usr/local/lib/cockroach",
	"Location where spatial related libraries can be found.",
)

func main() {
	flag.Parse()

	if _, err := geos.EnsureInit(geos.EnsureInitErrorDisplayPrivate, *flagGeoLibsDir); err != nil {
		log.Fatalf("could not initialize GEOS - spatial functions may not be available: %v", err)
	}

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/load", handleLoad)

	fmt.Printf("running server...\n")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

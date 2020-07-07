// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This package generated the hardcoded spatial ref sys mapping based
// on a given data set.
//
// Sample run:
// go run ./pkg/cmd/generate-spatial-ref-sys --src='/tmp/srids.csv' --dest='./pkg/geo/geoprojbase/projections.go' --template="./pkg/cmd/generate-spatial-ref-sys/generate.tmpl"

package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/geo/geoproj"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	flagSRC = flag.String(
		"src",
		"",
		"The source of where the spatial_ref_sys data lives. Assumes a CSV with no header row separated by ;.",
	)
	flagDEST = flag.String(
		"dest",
		"",
		"The resulting map file.",
	)
	flagTemplate = flag.String(
		"template",
		"generate.tmpl",
		"Which template file to use",
	)
)

type templateVars struct {
	Year        int
	Package     string
	Projections []projection
	Spheroids   []spheroid
}

type projection struct {
	SRID      string
	AuthName  string
	AuthSRID  string
	SRText    string
	Proj4Text string
	Bounds    *geoprojbase.Bounds

	IsLatLng        bool
	SpheroidVarName string
}

type spheroid struct {
	VarName    string
	MajorAxis  float64
	Flattening float64
}

func main() {
	flag.Parse()
	tmplVars := getTemplateVars()

	tmpl, err := template.ParseFiles(*flagTemplate)
	if err != nil {
		log.Fatal(err)
	}

	out, err := os.Create(*flagDEST)
	if err != nil {
		log.Fatal(err)
	}

	if err := tmpl.Execute(out, tmplVars); err != nil {
		log.Fatal(err)
	}

	if err := out.Close(); err != nil {
		log.Fatal(err)
	}

	if err := exec.Command("crlfmt", "-w", "-tab", "2", *flagDEST).Run(); err != nil {
		log.Fatal(err)
	}
}

func getTemplateVars() templateVars {
	type spheroidKey struct {
		majorAxis           float64
		eccentricitySquared float64
	}
	foundSpheroids := make(map[spheroidKey]int)
	foundSpheroidsInverse := make(map[int]spheroidKey)
	var projections []projection
	var spheroids []spheroid
	counter := 1
	for _, record := range readRecords() {
		proj4text := strings.TrimRight(record[4], " \t")
		isLatLng, s, err := geoproj.GetProjMetadata(geoprojbase.MakeProj4Text(proj4text))
		if err != nil {
			log.Fatal(err)
		}

		var spheroidVarName string
		key := spheroidKey{s.Radius, s.Flattening}
		if foundCounter, ok := foundSpheroids[key]; !ok {
			foundSpheroids[key] = counter
			foundSpheroidsInverse[counter] = key
			spheroidVarName = fmt.Sprintf(`spheroid%d`, counter)
			spheroids = append(
				spheroids,
				spheroid{
					VarName:    spheroidVarName,
					MajorAxis:  s.Radius,
					Flattening: s.Flattening,
				},
			)
			counter++
		} else {
			spheroidVarName = fmt.Sprintf(`spheroid%d`, foundCounter)
		}

		var bounds *geoprojbase.Bounds
		if record[1] == "EPSG" {
			var results struct {
				Results []struct {
					BBox []float64 `json:"bbox"`
				} `json:"results"`
			}
			for _, searchArgs := range []string{
				record[2],
				fmt.Sprintf("%s%%20deprecated%%3A1", record[2]), // some may be deprecated.
			} {
				resp, err := http.Get(fmt.Sprintf("http://epsg.io/?q=%s&format=json", searchArgs))
				if err != nil {
					log.Fatal(err)
				}

				body, err := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					log.Fatal(err)
				}

				if err := json.Unmarshal([]byte(body), &results); err != nil {
					log.Fatal(err)
				}
				if len(results.Results) == 1 {
					break
				}
			}

			if len(results.Results) != 1 {
				log.Fatal("WARNING: expected 1 result for %s, found %#v", record[2], results.Results)
			}
			bounds = &geoprojbase.Bounds{
				MinLat: results.Results[0].BBox[0],
				MaxLat: results.Results[0].BBox[1],
				MinLng: results.Results[0].BBox[2],
				MaxLng: results.Results[0].BBox[3],
			}
		}

		projections = append(
			projections,
			projection{
				SRID:      record[0],
				AuthName:  record[1],
				AuthSRID:  record[2],
				SRText:    record[3],
				Proj4Text: proj4text,

				Bounds:          bounds,
				IsLatLng:        isLatLng,
				SpheroidVarName: spheroidVarName,
			},
		)
	}
	pkgName := strings.Split(*flagDEST, "/")
	return templateVars{
		Year:        timeutil.Now().Year(),
		Package:     pkgName[len(pkgName)-2],
		Projections: projections,
		Spheroids:   spheroids,
	}
}

func readRecords() [][]string {
	in, err := os.Open(*flagSRC)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := in.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	r := csv.NewReader(in)
	r.Comma = ';'

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	return records
}

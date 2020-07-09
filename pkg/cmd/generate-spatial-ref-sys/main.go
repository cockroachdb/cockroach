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
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/geo/geoproj"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
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

type projectionBounds struct {
	MinX string
	MaxX string
	MinY string
	MaxY string
}

type projection struct {
	SRID      string
	AuthName  string
	AuthSRID  string
	SRText    string
	Proj4Text string
	Bounds    *projectionBounds

	IsLatLng        bool
	SpheroidVarName string
}

type spheroid struct {
	VarName    string
	MajorAxis  string
	Flattening string
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
					MajorAxis:  strconv.FormatFloat(s.Radius, 'f', -1, 64),
					Flattening: strconv.FormatFloat(s.Flattening, 'f', -1, 64),
				},
			)
			counter++
		} else {
			spheroidVarName = fmt.Sprintf(`spheroid%d`, foundCounter)
		}

		var bounds *projectionBounds
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
				resp, err := httputil.Get(context.Background(), fmt.Sprintf("http://epsg.io/?q=%s&format=json", searchArgs))
				if err != nil {
					log.Fatal(err)
				}

				body, err := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					log.Fatal(err)
				}

				if err := json.Unmarshal(body, &results); err != nil {
					log.Fatal(err)
				}
				if len(results.Results) == 1 {
					break
				}
			}

			if len(results.Results) != 1 {
				log.Fatalf("WARNING: expected 1 result for %s, found %#v", record[2], results.Results)
			}
			bbox := results.Results[0].BBox
			// We need to try against all 4 points of the polygon, as lat or lngs may stretch out
			// differently at the corners.
			xCoords := []float64{bbox[1], bbox[1], bbox[3], bbox[3]}
			yCoords := []float64{bbox[0], bbox[2], bbox[0], bbox[2]}
			if !isLatLng {
				if err := geoproj.Project(
					geoprojbase.MakeProj4Text("+proj=longlat +datum=WGS84 +no_defs"),
					geoprojbase.MakeProj4Text(proj4text),
					xCoords,
					yCoords,
					[]float64{0, 0, 0, 0},
				); err != nil {
					log.Fatal(err)
				}
			}
			sort.Slice(xCoords, func(i, j int) bool {
				return xCoords[i] < xCoords[j]
			})
			sort.Slice(yCoords, func(i, j int) bool {
				return yCoords[i] < yCoords[j]
			})
			bounds = &projectionBounds{
				MinX: strconv.FormatFloat(xCoords[0], 'f', -1, 64),
				MaxX: strconv.FormatFloat(xCoords[3], 'f', -1, 64),
				MinY: strconv.FormatFloat(yCoords[0], 'f', -1, 64),
				MaxY: strconv.FormatFloat(yCoords[3], 'f', -1, 64),
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

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
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach/pkg/geo/geoproj"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	foundSpheroids := make(map[spheroidKey]string)
	var mu syncutil.Mutex
	var projections []projection
	var spheroids []spheroid

	g := ctxgroup.WithContext(context.Background())
	records := readRecords()
	const numWorkers = 8
	batchSize := int(math.Ceil(float64(len(records)) / numWorkers))
	for i := 0; i < len(records); i += batchSize {
		start := i
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		g.GoCtx(func(ctx context.Context) error {
			for _, record := range records[start:end] {
				proj4text := strings.TrimRight(record[4], " \t")
				isLatLng, s, err := geoproj.GetProjMetadata(geoprojbase.MakeProj4Text(proj4text))
				if err != nil {
					log.Printf("error processing %s: %s, skipping", record[2], err)
					continue
				}

				key := spheroidKey{s.Radius, s.Flattening}
				mu.Lock()
				spheroidVarName, ok := foundSpheroids[key]
				if !ok {
					hash := sha256.Sum256([]byte(
						strconv.FormatFloat(s.Radius, 'f', -1, 64) + "," + strconv.FormatFloat(s.Flattening, 'f', -1, 64),
					))
					spheroidVarName = fmt.Sprintf(`spheroid%s`, strings.ToUpper(fmt.Sprintf("%x", hash[:6])))
					foundSpheroids[key] = spheroidVarName
					spheroids = append(
						spheroids,
						spheroid{
							VarName:    spheroidVarName,
							MajorAxis:  strconv.FormatFloat(s.Radius, 'f', -1, 64),
							Flattening: strconv.FormatFloat(s.Flattening, 'f', -1, 64),
						},
					)
				}
				mu.Unlock()

				var bounds *projectionBounds
				if record[1] == "EPSG" {
					var results struct {
						Results []struct {
							BBox interface{} `json:"bbox,omitempty"`
							Code string      `json:"code"`
						} `json:"results"`
					}
					for _, searchArgs := range []string{
						record[2],
						fmt.Sprintf("%s%%20deprecated%%3A1", record[2]), // some may be deprecated.
					} {
						var resp *http.Response
						for i := 0; i < 5; i++ {
							resp, err = httputil.Get(ctx, fmt.Sprintf("http://epsg.io/?q=%s&format=json", searchArgs))
							if err == nil {
								break
							}
							log.Printf("http failure on %s, retrying; %v", record[2], err)
							time.Sleep(time.Duration(i) * time.Second * 2)
						}
						if err != nil {
							return err
						}

						body, err := ioutil.ReadAll(resp.Body)
						resp.Body.Close()
						if err != nil {
							return err
						}

						if err := json.Unmarshal(body, &results); err != nil {
							return err
						}
						newResults := results.Results[:0]
						for i := range results.Results {
							if results.Results[i].Code == record[2] && results.Results[i].BBox != interface{}("") {
								newResults = append(newResults, results.Results[i])
							}
						}
						results.Results = newResults
						if len(results.Results) > 0 {
							break
						}
					}

					if len(results.Results) != 1 {
						log.Printf("WARNING: expected 1 result for %s, found %#v", record[2], results.Results)
					}
					bbox := results.Results[0].BBox.([]interface{})
					// We need to try against all 4 points of the polygon, as lat or lngs may stretch out
					// differently at the corners.
					xCoords := []float64{bbox[1].(float64), bbox[1].(float64), bbox[3].(float64), bbox[3].(float64)}
					yCoords := []float64{bbox[0].(float64), bbox[2].(float64), bbox[0].(float64), bbox[2].(float64)}
					if !isLatLng {
						if err := geoproj.Project(
							geoprojbase.MakeProj4Text("+proj=longlat +datum=WGS84 +no_defs"),
							geoprojbase.MakeProj4Text(proj4text),
							xCoords,
							yCoords,
							[]float64{0, 0, 0, 0},
						); err != nil {
							log.Printf("error processing %s: %s, skipping", record[2], err)
							continue
						}
					}

					sort.Slice(xCoords, func(i, j int) bool {
						return xCoords[i] < xCoords[j]
					})
					sort.Slice(yCoords, func(i, j int) bool {
						return yCoords[i] < yCoords[j]
					})
					skip := false
					for _, coord := range xCoords {
						if math.IsInf(coord, 1) || math.IsInf(coord, -1) {
							log.Printf("infinite coord at SRID %s, skipping", record[2])
							skip = true
							break
						}
					}
					if skip {
						continue
					}
					for _, coord := range yCoords {
						if math.IsInf(coord, 1) || math.IsInf(coord, -1) {
							log.Printf("infinite coord at SRID %s, skipping", record[2])
							skip = true
						}
					}
					if skip {
						continue
					}
					bounds = &projectionBounds{
						MinX: strconv.FormatFloat(xCoords[0], 'f', -1, 64),
						MaxX: strconv.FormatFloat(xCoords[3], 'f', -1, 64),
						MinY: strconv.FormatFloat(yCoords[0], 'f', -1, 64),
						MaxY: strconv.FormatFloat(yCoords[3], 'f', -1, 64),
					}
				}

				mu.Lock()
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
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
	sort.Slice(projections, func(i, j int) bool {
		l, err := strconv.ParseInt(projections[i].SRID, 0, 64)
		if err != nil {
			log.Fatal(err)
		}
		r, err := strconv.ParseInt(projections[j].SRID, 0, 64)
		if err != nil {
			log.Fatal(err)
		}
		return l < r
	})
	sort.Slice(spheroids, func(i, j int) bool {
		return spheroids[i].VarName < spheroids[j].VarName
	})
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

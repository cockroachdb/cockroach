// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package google

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const dashboardURL = "https://microbench.testeng.crdb.io/dashboard/"

// Service is capable of communicating with the Google Drive API and the Google
// Sheets API to create new spreadsheets and populate them from benchstat
// output.
type Service struct {
	drive  *drive.Service
	sheets *sheets.Service
}

// New creates a new Service. It verifies that credentials are properly set and
// returns an error if they are not.
func New(ctx context.Context) (*Service, error) {
	var srv Service
	var err error
	if srv.drive, err = newDriveService(ctx); err != nil {
		return nil, errors.Wrap(err, "retrieve Drive client")
	}
	if srv.sheets, err = newSheetsService(ctx); err != nil {
		return nil, errors.Wrap(err, "retrieve Sheets client")
	}
	if err = srv.testServices(ctx); err != nil {
		return nil, errors.Wrap(err, "testing Google clients")
	}
	return &srv, nil
}

// newDriveService constructs a new Google Drive service.
func newDriveService(ctx context.Context) (*drive.Service, error) {
	return drive.NewService(ctx, option.WithScopes(drive.DriveFileScope))
}

// newSheetsService constructs a new Google Sheets service.
func newSheetsService(ctx context.Context) (*sheets.Service, error) {
	return sheets.NewService(ctx, option.WithScopes(sheets.SpreadsheetsScope))
}

func (srv *Service) testServices(ctx context.Context) error {
	if _, err := srv.drive.About.Get().Fields("user").Context(ctx).Do(); err != nil {
		return errors.Wrap(err, "testing Drive client")
	}
	if _, err := srv.sheets.Spreadsheets.Get("none").Context(ctx).Do(); err != nil {
		// We expect a 404.
		var apiError *googleapi.Error
		if errors.As(err, &apiError) && apiError.Code != http.StatusNotFound {
			return errors.Wrap(err, "testing Sheets client")
		}
	}
	return nil
}

// CreateSheet creates a new Google spreadsheet with the provided metric data.
func (srv *Service) CreateSheet(
	ctx context.Context,
	name string,
	comparisonResults []*model.ComparisonResult,
	oldID, newID string,
) (string, error) {
	var s sheets.Spreadsheet
	s.Properties = &sheets.SpreadsheetProperties{Title: name}

	// Raw data sheets.
	sheetInfos, idx := make([]rawSheetInfo, 0, len(comparisonResults)), 0
	for _, result := range comparisonResults {
		metric := result.Metric
		comparisons := make(map[string]*model.Comparison)
		for _, detail := range result.Comparisons {
			comparisons[detail.BenchmarkName] = detail.Comparison
		}

		// Only generate a sheet if there are comparisons to show.
		if len(comparisons) != 0 {
			sh, info := srv.createRawSheet(metric, comparisons, oldID, newID, idx)
			s.Sheets = append(s.Sheets, sh)
			sheetInfos = append(sheetInfos, info)
			idx++
		}
	}

	// Pivot table overview sheet. Place in front.
	overview := srv.createOverviewSheet(sheetInfos)
	s.Sheets = append([]*sheets.Sheet{overview}, s.Sheets...)

	// Create the spreadsheet.
	res, err := srv.createSheet(ctx, s)
	if err != nil {
		return "", err
	}

	// Update the new spreadsheet's permissions.
	if err := srv.updatePerms(ctx, res.SpreadsheetId); err != nil {
		return "", err
	}

	return res.SpreadsheetUrl, nil
}

type rawSheetInfo struct {
	id          int64
	metric      *model.Metric
	grid        *sheets.GridProperties
	deltaCol    int64
	nonZeroVals []string
}

// createRawSheet creates a new sheet that corresponds to the raw metric data in
// a single benchstat table. The sheet is formatted like:
//
//	+------------+---------------------+---------------------+---------+-----------------+
//	| name       | old time/op (ns/op) | new time/op (ns/op) | delta   | note            |
//	+------------+---------------------+---------------------+---------+-----------------+
//	| Benchmark1 |            290026.2 |              290075 | -34.29% | (p=0.008 n=5+5) |
//	| Benchmark2 |               15588 |             15717.6 |  ~      | (p=0.841 n=5+5) |
//	                                          ...
func (srv *Service) createRawSheet(
	metric *model.Metric, comparisons map[string]*model.Comparison, oldID, newID string, tIdx int,
) (*sheets.Sheet, rawSheetInfo) {
	sheetID := sheetIDForTable(tIdx)
	runs := []string{oldID, newID}

	var info rawSheetInfo
	info.metric = metric
	info.id = sheetID

	props := &sheets.SheetProperties{
		Title:   "Raw: " + metric.Name,
		SheetId: sheetID,
	}

	var data []*sheets.RowData
	var metadata []*sheets.DimensionProperties
	var numCols int64

	// Header row.
	{
		var vals []*sheets.CellData

		// Column: Benchmark name.
		vals = append(vals, strCell("name"))
		metadata = append(metadata, withSize(600))

		// Columns: Metric names.
		for _, run := range runs {
			name := fmt.Sprintf("%s %s (%s)", run, metric.Name, metric.Unit)
			vals = append(vals, strCell(name))
			metadata = append(metadata, withSize(150))
		}

		// Column: delta.
		info.deltaCol = int64(len(vals))
		vals = append(vals, strCell("delta"))
		metadata = append(metadata, withSize(100))

		// Column: note.
		vals = append(vals, strCell("note"))
		metadata = append(metadata, withSize(150))

		numCols = int64(len(vals))
		data = append(data, &sheets.RowData{Values: vals})
	}

	// Sort comparisons by delta, or the benchmark name if no delta is available.
	keys := maps.Keys(comparisons)
	sort.Slice(keys, func(i, j int) bool {
		d1 := comparisons[keys[i]].Delta * float64(metric.Better)
		d2 := comparisons[keys[j]].Delta * float64(metric.Better)
		if d1 == d2 {
			return keys[i] < keys[j]
		}
		return d1 > d2
	})

	// Data rows.
	for _, name := range keys {
		entry := metric.BenchmarkEntries[name]
		comparison := comparisons[name]
		var vals []*sheets.CellData
		vals = append(vals, strCellWithLink(name, dashboardLink(name)))
		for _, run := range runs {
			vals = append(vals, numCell(entry.Summaries[run].Center))
		}
		delta := comparison.FormattedDelta
		if delta == "~" || delta == "?" {
			vals = append(vals, strCell(delta))
		} else {
			vals = append(vals, percentCell(deltaToNum(delta)))
			info.nonZeroVals = append(info.nonZeroVals, deltaToPercentString(delta))
		}
		vals = append(vals, strCell(comparison.Distribution.String()))
		data = append(data, &sheets.RowData{Values: vals})
	}

	// Conditional formatting.
	cf := condFormatting(sheetID, info.deltaCol, metric.Better < 0)

	// Grid properties.
	grid := &sheets.GridProperties{
		ColumnCount:    numCols,
		RowCount:       int64(len(data)),
		FrozenRowCount: 1,
	}
	info.grid = grid
	props.GridProperties = grid

	// Construct the new sheet.
	sheet := &sheets.Sheet{
		Properties: props,
		Data: []*sheets.GridData{{
			RowData:        data,
			ColumnMetadata: metadata,
		}},
		ConditionalFormats: []*sheets.ConditionalFormatRule{cf},
	}
	return sheet, info
}

// createOverviewSheet creates a new sheet that contains an overview of all raw
// metric data using pivot tables. The sheet is formatted like:
//
//	+------------+---------+----+------------+----------+
//	| name       | time/op |    | name       | alloc/op |
//	+------------+---------+----+------------+----------+
//	| Benchmark1 | -34.29% |    | Benchmark3 | -12.99%  |
//	| Benchmark2 |   4.02% |    | Benchmark4 |   0.11%  |
//	                         ...
func (srv *Service) createOverviewSheet(rawInfos []rawSheetInfo) *sheets.Sheet {
	const title = "Overview: Significant Changes"
	sheetID := sheetIDForTable(len(rawInfos))
	props := &sheets.SheetProperties{
		Title:   title,
		SheetId: sheetID,
	}

	var vals []*sheets.CellData
	var metadata []*sheets.DimensionProperties
	var cfs []*sheets.ConditionalFormatRule

	for i, info := range rawInfos {
		if i != 0 {
			metadata = append(metadata, withSize(50))
			vals = append(vals, &sheets.CellData{})
		}

		// If there were no significant changes in this table, don't create
		// a pivot table.
		if len(info.nonZeroVals) == 0 {
			noChanges := fmt.Sprintf("no change in %s", info.metric.Name)
			vals = append(vals, strCell(noChanges))
			metadata = append(metadata, withSize(400))
			continue
		}

		smallerBetter := info.metric.Better < 0
		sortOrder := "DESCENDING"
		if smallerBetter {
			sortOrder = "ASCENDING"
		}
		vals = append(vals, &sheets.CellData{
			PivotTable: &sheets.PivotTable{
				Source: &sheets.GridRange{
					SheetId:          sheetIDForTable(i),
					StartColumnIndex: 0,
					EndColumnIndex:   info.grid.ColumnCount,
					StartRowIndex:    0,
					EndRowIndex:      info.grid.RowCount,
				},
				Rows: []*sheets.PivotGroup{{
					SourceColumnOffset: 0,
					SortOrder:          sortOrder,
					ValueBucket: &sheets.PivotGroupSortValueBucket{
						ValuesIndex: 0,
					},
					ForceSendFields: []string{"SourceColumnOffset"},
				}},
				Values: []*sheets.PivotValue{{
					SourceColumnOffset: info.deltaCol,
					Name:               info.metric.Name,
					SummarizeFunction:  "AVERAGE",
				}},
				Criteria: map[string]sheets.PivotFilterCriteria{
					strconv.Itoa(int(info.deltaCol)): {
						VisibleValues: info.nonZeroVals,
					},
				},
			},
		})
		vals = append(vals, &sheets.CellData{})
		metadata = append(metadata, withSize(400), withSize(100))

		deltaCol := int64(len(vals)) - 1
		cf := condFormatting(props.SheetId, deltaCol, smallerBetter)
		cfs = append(cfs, cf)
	}

	// Construct the new sheet.
	props.GridProperties = &sheets.GridProperties{
		ColumnCount:    int64(len(vals)),
		FrozenRowCount: 1,
	}
	data := []*sheets.RowData{{
		Values: vals,
	}}
	pivot := &sheets.Sheet{
		Properties: props,
		Data: []*sheets.GridData{{
			RowData:        data,
			ColumnMetadata: metadata,
		}},
		ConditionalFormats: cfs,
	}
	return pivot
}

func (srv *Service) createSheet(
	ctx context.Context, s sheets.Spreadsheet,
) (*sheets.Spreadsheet, error) {
	res, err := srv.sheets.Spreadsheets.Create(&s).Context(ctx).Do()
	if err != nil {
		return nil, errors.Wrap(err, "create new Spreadsheet")
	}
	return res, nil
}

func (srv *Service) updatePerms(ctx context.Context, spreadsheetID string) error {
	// Update the new spreadsheet's permissions. By default, the spreadsheet is
	// owned by the Service Account that the Sheets service was authenticated
	// with, and is not accessible to anyone else. We open the file up to anyone
	// with the link.
	perm := &drive.Permission{
		Type: "anyone",
		Role: "reader",
	}
	_, err := srv.drive.Permissions.Create(spreadsheetID, perm).Context(ctx).Do()
	return errors.Wrap(err, "update Spreadsheet permissions")
}

func sheetIDForTable(i int) int64 {
	return int64(i + 1)
}

func strCell(s string) *sheets.CellData {
	return &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			StringValue: &s,
		},
	}
}

func strCellWithLink(s string, link string) *sheets.CellData {
	return &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			StringValue: &s,
		},
		TextFormatRuns: []*sheets.TextFormatRun{
			{
				StartIndex: 0,
				Format: &sheets.TextFormat{
					Link: &sheets.Link{
						Uri: link,
					},
				},
			},
		},
	}
}

func dashboardLink(name string) string {
	parts := strings.Split(name, util.PackageSeparator)
	return dashboardURL + "?benchmark=" + parts[1] + "&package=" + parts[0]
}

func numCell(f float64) *sheets.CellData {
	switch {
	case math.IsInf(f, +1):
		f = math.MaxFloat64
	case math.IsInf(f, -1):
		f = -math.MaxFloat64
	}
	return &sheets.CellData{
		UserEnteredValue: &sheets.ExtendedValue{
			NumberValue: &f,
		},
	}
}

func percentCell(f float64) *sheets.CellData {
	c := numCell(f)
	c.UserEnteredFormat = &sheets.CellFormat{
		NumberFormat: &sheets.NumberFormat{Type: "PERCENT"},
	}
	return c
}

func deltaToNum(delta string) float64 {
	delta = strings.TrimRight(delta, `%`)
	f, err := strconv.ParseFloat(delta, 64)
	if err != nil {
		panic(err)
	}
	return f / 100
}

func deltaToPercentString(delta string) string {
	delta = strings.TrimLeft(delta, "+")
	for strings.Contains(delta, `.`) {
		if strings.Contains(delta, `.%`) {
			delta = strings.ReplaceAll(delta, `.%`, `%`)
		} else if strings.Contains(delta, `0%`) {
			delta = strings.ReplaceAll(delta, `0%`, `%`)
		} else {
			break
		}
	}
	return delta
}

func withSize(pixels int64) *sheets.DimensionProperties {
	return &sheets.DimensionProperties{PixelSize: pixels}
}

var red = rgb(230, 124, 115)
var white = rgb(255, 255, 255)
var green = rgb(87, 187, 138)

func rgb(red, green, blue int) *sheets.Color {
	return &sheets.Color{
		Red:   float64(red) / 255,
		Green: float64(green) / 255,
		Blue:  float64(blue) / 255,
	}
}

func condFormatting(sheetID, col int64, smallerBetter bool) *sheets.ConditionalFormatRule {
	minColor, maxColor := red, green
	if smallerBetter {
		minColor, maxColor = maxColor, minColor
	}
	return &sheets.ConditionalFormatRule{
		GradientRule: &sheets.GradientRule{
			Minpoint: &sheets.InterpolationPoint{
				Color: minColor,
				Type:  "NUMBER",
				Value: "-1.0",
			},
			Midpoint: &sheets.InterpolationPoint{
				Color: white,
				Type:  "NUMBER",
				Value: "0.0",
			},
			Maxpoint: &sheets.InterpolationPoint{
				Color: maxColor,
				Type:  "NUMBER",
				Value: "1.0",
			},
		},
		Ranges: []*sheets.GridRange{{
			SheetId:          sheetID,
			StartColumnIndex: col,
			EndColumnIndex:   col + 1,
			StartRowIndex:    1,
		}},
	}
}

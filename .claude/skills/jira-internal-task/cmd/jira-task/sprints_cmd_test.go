package main

import (
	"encoding/json"
	"testing"
)

func TestFormatSprintListJSON(t *testing.T) {
	sprints := []Sprint{
		{ID: 10, Name: "KV Sprint A", State: "active", StartDate: "2026-01-01T00:00:00Z", EndDate: "2026-01-15T00:00:00Z", OriginBoardID: 400},
		{ID: 11, Name: "KV Holding pen", State: "future", StartDate: "2026-01-16T00:00:00Z", EndDate: "2026-01-30T00:00:00Z", OriginBoardID: 400},
		{ID: 12, Name: "Other team", State: "active", StartDate: "2026-01-01T00:00:00Z", EndDate: "2026-01-15T00:00:00Z", OriginBoardID: 999},
	}

	items := formatSprintList(sprints, 400)
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	expected := `[
  {
    "id": 10,
    "name": "KV Sprint A",
    "state": "active",
    "startDate": "2026-01-01T00:00:00Z",
    "endDate": "2026-01-15T00:00:00Z"
  },
  {
    "id": 11,
    "name": "KV Holding pen",
    "state": "future",
    "startDate": "2026-01-16T00:00:00Z",
    "endDate": "2026-01-30T00:00:00Z"
  }
]`
	if string(data) != expected {
		t.Fatalf("unexpected JSON output:\n%s", string(data))
	}
}

func TestFindSprintByNameAcrossStates(t *testing.T) {
	sprints := []Sprint{
		{ID: 20, Name: "KV Sprint A", State: "active"},
		{ID: 21, Name: "KV Holding pen", State: "future"},
	}

	info, err := findSprintByName(sprints, "Holding pen")
	if err != nil {
		t.Fatalf("expected match, got error: %v", err)
	}
	if info.ID != 21 {
		t.Fatalf("expected sprint ID 21, got %d", info.ID)
	}
}

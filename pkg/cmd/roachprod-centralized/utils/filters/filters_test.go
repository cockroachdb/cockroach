// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package filters

import (
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test object to use in memory filter tests
type TestTask struct {
	ID        string    `db:"id"`
	Type      string    `db:"type"`
	State     string    `db:"state"`
	Count     int       `db:"count"`
	Score     float64   `db:"score"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"creation_datetime"`
	UpdatedAt time.Time
}

func TestFilterOperatorValidation(t *testing.T) {
	tests := []struct {
		name        string
		filter      types.FieldFilter
		expectError bool
	}{
		{
			name: "valid equal filter",
			filter: types.FieldFilter{
				Field:    "State",
				Operator: types.OpEqual,
				Value:    "pending",
			},
			expectError: false,
		},
		{
			name: "valid in filter",
			filter: types.FieldFilter{
				Field:    "State",
				Operator: types.OpIn,
				Value:    []string{"pending", "running"},
			},
			expectError: false,
		},
		{
			name: "invalid empty field",
			filter: types.FieldFilter{
				Field:    "",
				Operator: types.OpEqual,
				Value:    "test",
			},
			expectError: true,
		},
		{
			name: "invalid nil value for equal",
			filter: types.FieldFilter{
				Field:    "State",
				Operator: types.OpEqual,
				Value:    nil,
			},
			expectError: true,
		},
		{
			name: "invalid non-slice for in operator",
			filter: types.FieldFilter{
				Field:    "State",
				Operator: types.OpIn,
				Value:    "single_value",
			},
			expectError: true,
		},
		{
			name: "invalid empty slice for in operator",
			filter: types.FieldFilter{
				Field:    "State",
				Operator: types.OpIn,
				Value:    []string{},
			},
			expectError: true,
		},
		{
			name: "invalid operator",
			filter: types.FieldFilter{
				Field:    "State",
				Operator: "invalid_op",
				Value:    "test",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.filter.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFilterSetValidation(t *testing.T) {
	tests := []struct {
		name        string
		filterSet   *types.FilterSet
		expectError bool
	}{
		{
			name:        "nil filter set",
			filterSet:   nil,
			expectError: false,
		},
		{
			name:        "empty filter set",
			filterSet:   NewFilterSet(),
			expectError: false,
		},
		{
			name:        "valid filter set",
			filterSet:   NewFilterSet().AddFilter("State", types.OpEqual, "pending"),
			expectError: false,
		},
		{
			name: "invalid logic operator",
			filterSet: &types.FilterSet{
				Logic: "invalid_logic",
				Filters: []types.FieldFilter{
					{Field: "State", Operator: types.OpEqual, Value: "pending"},
				},
			},
			expectError: true,
		},
		{
			name: "invalid filter in set",
			filterSet: &types.FilterSet{
				Logic: types.LogicAnd,
				Filters: []types.FieldFilter{
					{Field: "", Operator: types.OpEqual, Value: "pending"},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.filterSet.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetSupportedOperatorsForType(t *testing.T) {
	tests := []struct {
		name        string
		goType      reflect.Type
		expectedOps []types.FilterOperator
	}{
		{
			name:        "string type",
			goType:      reflect.TypeOf(""),
			expectedOps: []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpIn, types.OpNotIn, types.OpLike, types.OpNotLike},
		},
		{
			name:        "int type",
			goType:      reflect.TypeOf(0),
			expectedOps: []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpLess, types.OpLessEq, types.OpGreater, types.OpGreaterEq, types.OpIn, types.OpNotIn},
		},
		{
			name:        "float64 type",
			goType:      reflect.TypeOf(0.0),
			expectedOps: []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpLess, types.OpLessEq, types.OpGreater, types.OpGreaterEq, types.OpIn, types.OpNotIn},
		},
		{
			name:        "bool type",
			goType:      reflect.TypeOf(true),
			expectedOps: []types.FilterOperator{types.OpEqual, types.OpNotEqual},
		},
		{
			name:        "time.Time type",
			goType:      reflect.TypeOf(time.Time{}),
			expectedOps: []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpLess, types.OpLessEq, types.OpGreater, types.OpGreaterEq, types.OpIn, types.OpNotIn},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ops := GetSupportedOperatorsForType(tt.goType)
			assert.ElementsMatch(t, tt.expectedOps, ops)
		})
	}
}

func TestValidateOperatorForType(t *testing.T) {
	tests := []struct {
		name        string
		operator    types.FilterOperator
		goType      reflect.Type
		expectError bool
	}{
		{
			name:        "valid string equal",
			operator:    types.OpEqual,
			goType:      reflect.TypeOf(""),
			expectError: false,
		},
		{
			name:        "valid int less than",
			operator:    types.OpLess,
			goType:      reflect.TypeOf(0),
			expectError: false,
		},
		{
			name:        "invalid string less than",
			operator:    types.OpLess,
			goType:      reflect.TypeOf(""),
			expectError: true,
		},
		{
			name:        "invalid bool less than",
			operator:    types.OpLess,
			goType:      reflect.TypeOf(true),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOperatorForType(tt.operator, tt.goType)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSQLQueryBuilder(t *testing.T) {
	tests := []struct {
		name          string
		filterSet     *types.FilterSet
		expectedWhere string
		expectedArgs  []interface{}
		expectError   bool
	}{
		{
			name:          "nil filter set",
			filterSet:     nil,
			expectedWhere: "",
			expectedArgs:  nil,
			expectError:   false,
		},
		{
			name:          "empty filter set",
			filterSet:     NewFilterSet(),
			expectedWhere: "",
			expectedArgs:  nil,
			expectError:   false,
		},
		{
			name:          "single equal filter",
			filterSet:     NewFilterSet().AddFilter("State", types.OpEqual, "pending"),
			expectedWhere: "WHERE state = $1",
			expectedArgs:  []interface{}{"pending"},
			expectError:   false,
		},
		{
			name: "multiple AND filters",
			filterSet: NewFilterSet().
				AddFilter("State", types.OpEqual, "pending").
				AddFilter("Type", types.OpEqual, "backup").
				AddFilter("CreatedAt", types.OpEqual, "2006-01-02T15:04:05Z"),
			expectedWhere: "WHERE state = $1 AND type = $2 AND creation_datetime = $3",
			expectedArgs:  []interface{}{"pending", "backup", "2006-01-02T15:04:05Z"},
			expectError:   false,
		},
		{
			name: "multiple OR filters",
			filterSet: NewFilterSet().
				SetLogic(types.LogicOr).
				AddFilter("State", types.OpEqual, "pending").
				AddFilter("State", types.OpEqual, "running"),
			expectedWhere: "WHERE state = $1 OR state = $2",
			expectedArgs:  []interface{}{"pending", "running"},
			expectError:   false,
		},
		{
			name: "comparison operators",
			filterSet: NewFilterSet().
				AddFilter("CreatedAt", types.OpGreater, "2024-01-01").
				AddFilter("Count", types.OpLessEq, 100),
			expectedWhere: "WHERE creation_datetime > $1 AND count <= $2",
			expectedArgs:  []interface{}{"2024-01-01", 100},
			expectError:   false,
		},
		{
			name:          "IN operator",
			filterSet:     NewFilterSet().AddFilter("State", types.OpIn, []string{"pending", "running", "done"}),
			expectedWhere: "WHERE state IN ($1, $2, $3)",
			expectedArgs:  []interface{}{"pending", "running", "done"},
			expectError:   false,
		},
		{
			name:          "NOT IN operator",
			filterSet:     NewFilterSet().AddFilter("State", types.OpNotIn, []string{"failed", "cancelled"}),
			expectedWhere: "WHERE state NOT IN ($1, $2)",
			expectedArgs:  []interface{}{"failed", "cancelled"},
			expectError:   false,
		},
		{
			name:          "LIKE operator",
			filterSet:     NewFilterSet().AddFilter("Type", types.OpLike, "%backup%"),
			expectedWhere: "WHERE type LIKE $1",
			expectedArgs:  []interface{}{"%backup%"},
			expectError:   false,
		},
		{
			name: "mixed operators",
			filterSet: NewFilterSet().
				AddFilter("State", types.OpNotEqual, "failed").
				AddFilter("Type", types.OpIn, []string{"backup", "restore"}).
				AddFilter("CreatedAt", types.OpGreater, "2024-01-01"),
			expectedWhere: "WHERE state != $1 AND type IN ($2, $3) AND creation_datetime > $4",
			expectedArgs:  []interface{}{"failed", "backup", "restore", "2024-01-01"},
			expectError:   false,
		},
		{
			name: "invalid field name",
			filterSet: NewFilterSet().
				AddFilter("NonExistentField", types.OpEqual, "value"),
			expectedWhere: "",
			expectedArgs:  nil,
			expectError:   true,
		},
		{
			name: "field name with no db tag defaults to snake_case",
			filterSet: NewFilterSet().
				AddFilter("UpdatedAt", types.OpEqual, "2024-01-02T15:04:05Z"),
			expectedWhere: "WHERE updated_at = $1",
			expectedArgs:  []interface{}{"2024-01-02T15:04:05Z"},
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewSQLQueryBuilderWithTypeHint(reflect.TypeOf(TestTask{}))
			whereClause, args, err := qb.BuildWhere(tt.filterSet)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedWhere, whereClause)
				assert.Equal(t, tt.expectedArgs, args)
			}
		})
	}
}

func TestMemoryFilterEvaluator_StructFields(t *testing.T) {
	evaluator := NewMemoryFilterEvaluatorWithTypeHint(reflect.TypeOf(TestTask{}))
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	task := TestTask{
		ID:        "task-123",
		Type:      "backup",
		State:     "pending",
		Count:     42,
		Score:     85.5,
		Active:    true,
		CreatedAt: testTime,
		UpdatedAt: testTime.Add(time.Hour),
	}

	tests := []struct {
		name        string
		filterSet   *types.FilterSet
		expected    bool
		expectError bool
	}{
		{
			name:      "nil filter set should match",
			filterSet: nil,
			expected:  true,
		},
		{
			name:      "empty filter set should match",
			filterSet: NewFilterSet(),
			expected:  true,
		},
		{
			name:      "string equal match",
			filterSet: NewFilterSet().AddFilter("State", types.OpEqual, "pending"),
			expected:  true,
		},
		{
			name:      "string equal no match",
			filterSet: NewFilterSet().AddFilter("State", types.OpEqual, "running"),
			expected:  false,
		},
		{
			name:      "string not equal match",
			filterSet: NewFilterSet().AddFilter("State", types.OpNotEqual, "running"),
			expected:  true,
		},
		{
			name:      "int equal match",
			filterSet: NewFilterSet().AddFilter("Count", types.OpEqual, 42),
			expected:  true,
		},
		{
			name:      "int less than match",
			filterSet: NewFilterSet().AddFilter("Count", types.OpLess, 50),
			expected:  true,
		},
		{
			name:      "int greater than no match",
			filterSet: NewFilterSet().AddFilter("Count", types.OpGreater, 50),
			expected:  false,
		},
		{
			name:      "float comparison match",
			filterSet: NewFilterSet().AddFilter("Score", types.OpGreaterEq, 85.0),
			expected:  true,
		},
		{
			name:      "bool equal match",
			filterSet: NewFilterSet().AddFilter("Active", types.OpEqual, true),
			expected:  true,
		},
		{
			name:      "time comparison match",
			filterSet: NewFilterSet().AddFilter("CreatedAt", types.OpLess, testTime.Add(time.Hour)),
			expected:  true,
		},
		{
			name:      "string IN match",
			filterSet: NewFilterSet().AddFilter("State", types.OpIn, []string{"pending", "running"}),
			expected:  true,
		},
		{
			name:      "string NOT IN match",
			filterSet: NewFilterSet().AddFilter("State", types.OpNotIn, []string{"done", "failed"}),
			expected:  true,
		},
		{
			name:      "string LIKE match",
			filterSet: NewFilterSet().AddFilter("Type", types.OpLike, "back"),
			expected:  true,
		},
		{
			name: "multiple AND filters match",
			filterSet: NewFilterSet().
				AddFilter("State", types.OpEqual, "pending").
				AddFilter("Count", types.OpGreater, 40),
			expected: true,
		},
		{
			name: "multiple AND filters no match",
			filterSet: NewFilterSet().
				AddFilter("State", types.OpEqual, "pending").
				AddFilter("Count", types.OpGreater, 50),
			expected: false,
		},
		{
			name: "multiple OR filters match",
			filterSet: NewFilterSet().
				SetLogic(types.LogicOr).
				AddFilter("State", types.OpEqual, "running").
				AddFilter("Count", types.OpEqual, 42),
			expected: true,
		},
		{
			name: "multiple OR filters no match",
			filterSet: NewFilterSet().
				SetLogic(types.LogicOr).
				AddFilter("State", types.OpEqual, "running").
				AddFilter("Count", types.OpEqual, 100),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(task, tt.filterSet)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMemoryFilterEvaluator_ErrorCases(t *testing.T) {
	evaluator := NewMemoryFilterEvaluatorWithTypeHint(reflect.TypeOf(TestTask{}))
	task := TestTask{ID: "test", Type: "backup", State: "pending"}

	tests := []struct {
		name        string
		filterSet   *types.FilterSet
		expectError bool
	}{
		{
			name:        "nonexistent field",
			filterSet:   NewFilterSet().AddFilter("nonexistent_field", types.OpEqual, "value"),
			expectError: true,
		},
		{
			name:        "invalid IN value type",
			filterSet:   NewFilterSet().AddFilter("State", types.OpIn, "not_a_slice"),
			expectError: true,
		},
		{
			name:        "LIKE on non-string field",
			filterSet:   NewFilterSet().AddFilter("Count", types.OpLike, "pattern"),
			expectError: true,
		},
		{
			name:        "incompatible type comparison",
			filterSet:   NewFilterSet().AddFilter("State", types.OpLess, 42),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := evaluator.Evaluate(task, tt.filterSet)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFilterSetBuilderMethods(t *testing.T) {
	// Test fluent API
	fs := NewFilterSet().
		AddFilter("State", types.OpEqual, "pending").
		AddFilter("Type", types.OpIn, []string{"backup", "restore"}).
		SetLogic(types.LogicOr)

	assert.Equal(t, types.LogicOr, fs.Logic)
	assert.Len(t, fs.Filters, 2)
	assert.Equal(t, "State", fs.Filters[0].Field)
	assert.Equal(t, types.OpEqual, fs.Filters[0].Operator)
	assert.Equal(t, "pending", fs.Filters[0].Value)

	// Test IsEmpty
	emptyFS := NewFilterSet()
	assert.True(t, emptyFS.IsEmpty())
	assert.False(t, fs.IsEmpty())
}

func TestMemoryFilterEvaluator_EdgeCases(t *testing.T) {
	evaluator := NewMemoryFilterEvaluatorWithTypeHint(reflect.TypeOf(TestTask{}))

	// Test with pointer to struct
	task := &TestTask{
		ID:    "test",
		Type:  "backup",
		State: "pending",
		Count: 10,
	}

	filterSet := NewFilterSet().AddFilter("State", types.OpEqual, "pending")
	result, err := evaluator.Evaluate(task, filterSet)

	require.NoError(t, err)
	assert.True(t, result)

	// Test case sensitivity in LIKE
	likeFilter := NewFilterSet().AddFilter("Type", types.OpLike, "BACK")
	result, err = evaluator.Evaluate(task, likeFilter)

	require.NoError(t, err)
	assert.True(t, result) // Should match because we do case-insensitive comparison
}

func TestMemoryFilterEvaluator_NumericComparisons(t *testing.T) {
	evaluator := NewMemoryFilterEvaluatorWithTypeHint(reflect.TypeOf(TestTask{}))

	task := TestTask{
		Count: 42,
		Score: 85.5,
	}

	tests := []struct {
		name     string
		field    string
		operator types.FilterOperator
		value    interface{}
		expected bool
	}{
		// Int comparisons
		{"int equal", "Count", types.OpEqual, 42, true},
		{"int not equal", "Count", types.OpNotEqual, 41, true},
		{"int less", "Count", types.OpLess, 50, true},
		{"int less equal", "Count", types.OpLessEq, 42, true},
		{"int greater", "Count", types.OpGreater, 40, true},
		{"int greater equal", "Count", types.OpGreaterEq, 42, true},

		// Float comparisons
		{"float equal", "Score", types.OpEqual, 85.5, true},
		{"float less", "Score", types.OpLess, 90.0, true},
		{"float greater", "Score", types.OpGreater, 80.0, true},

		// Mixed numeric comparisons
		{"int vs float", "Count", types.OpLess, 42.5, true},
		{"float vs int", "Score", types.OpGreater, 80, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet := NewFilterSet().AddFilter(tt.field, tt.operator, tt.value)
			result, err := evaluator.Evaluate(task, filterSet)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddFilter_SliceValueHandling(t *testing.T) {
	tests := []struct {
		name          string
		operator      types.FilterOperator
		inputValue    interface{}
		expectedValue interface{}
		expectedError bool
	}{
		{
			name:          "IN operator with slice - keeps slice",
			operator:      types.OpIn,
			inputValue:    []string{"pending", "running"},
			expectedValue: []string{"pending", "running"},
		},
		{
			name:          "NOT_IN operator with slice - keeps slice",
			operator:      types.OpNotIn,
			inputValue:    []string{"failed", "done"},
			expectedValue: []string{"failed", "done"},
		},
		{
			name:          "Equal operator with slice - extracts first element",
			operator:      types.OpEqual,
			inputValue:    []string{"pending"},
			expectedValue: "pending",
		},
		{
			name:          "NotEqual operator with slice - extracts first element",
			operator:      types.OpNotEqual,
			inputValue:    []string{"failed"},
			expectedValue: "failed",
		},
		{
			name:          "Greater operator with slice - extracts first element",
			operator:      types.OpGreater,
			inputValue:    []int{10},
			expectedValue: 10,
		},
		{
			name:          "Equal operator with empty slice - keeps empty slice",
			operator:      types.OpEqual,
			inputValue:    []string{},
			expectedValue: []string{},
		},
		{
			name:          "Equal operator with single value - keeps single value",
			operator:      types.OpEqual,
			inputValue:    "pending",
			expectedValue: "pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet := NewFilterSet()
			filterSet.AddFilter("TestField", tt.operator, tt.inputValue)

			require.Equal(t, 1, len(filterSet.Filters))
			filter := filterSet.Filters[0]

			assert.Equal(t, "TestField", filter.Field)
			assert.Equal(t, tt.operator, filter.Operator)
			assert.Equal(t, tt.expectedValue, filter.Value)
		})
	}
}

func TestSQLQueryBuilder_TimezoneHandling(t *testing.T) {
	// Test in SQL query building context with TIMESTAMPTZ columns
	t.Run("timezone preservation in SQL query", func(t *testing.T) {
		filterSet := NewFilterSet()

		// Add a filter with timezone-aware time
		estTime := mustParseTime(t, "2025-09-16T14:00:00-05:00")
		filterSet.AddFilter("CreatedAt", types.OpGreater, estTime)

		qb := NewSQLQueryBuilderWithTypeHint(reflect.TypeOf(TestTask{}))
		whereClause, args, err := qb.BuildWhere(filterSet)

		require.NoError(t, err)
		assert.Equal(t, "WHERE creation_datetime > $1", whereClause)
		require.Len(t, args, 1)

		// Verify the argument preserves timezone information (for TIMESTAMPTZ columns)
		originalTime, ok := args[0].(time.Time)
		require.True(t, ok, "SQL argument should be time.Time")

		// The original timezone-aware time should be preserved for TIMESTAMPTZ columns
		assert.True(t, originalTime.Equal(estTime),
			"SQL argument should preserve timezone-aware time")
	})
}

// mustParseTime is a helper that parses a time string and fails the test if parsing fails
func mustParseTime(t *testing.T, timeStr string) time.Time {
	parsed, err := time.Parse(time.RFC3339, timeStr)
	require.NoError(t, err, "Failed to parse time: %s", timeStr)
	return parsed
}

// Define types for testing custom string type filtering
type TestTaskState string
type TestTaskType string

const (
	TestStatePending TestTaskState = "pending"
	TestStateRunning TestTaskState = "running"
	TestStateDone    TestTaskState = "done"
	TestStateFailed  TestTaskState = "failed"

	TestTypeBackup TestTaskType = "backup"
	TestTypeSync   TestTaskType = "sync"
)

// MockTask represents a task object that uses custom string types
type MockTask struct {
	ID    string
	State TestTaskState
	Type  TestTaskType
}

// GetState returns the task state (simulates ITask interface)
func (task *MockTask) GetState() TestTaskState { return task.State }

// GetType returns the task type (simulates ITask interface)
func (task *MockTask) GetType() TestTaskType { return task.Type }

// TestCustomStringTypeFiltering tests that custom string types work correctly with memory filtering
// This ensures the fixes for custom string type handling work end-to-end
func TestCustomStringTypeFiltering(t *testing.T) {

	tests := []struct {
		name        string
		task        *MockTask
		filters     func() *types.FilterSet
		shouldMatch bool
	}{
		{
			name: "custom type equal filter - match",
			task: &MockTask{ID: "1", State: TestStateDone, Type: TestTypeBackup},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpEqual, TestStateDone)
				return fs
			},
			shouldMatch: true,
		},
		{
			name: "custom type equal filter - no match",
			task: &MockTask{ID: "2", State: TestStateRunning, Type: TestTypeBackup},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpEqual, TestStateDone)
				return fs
			},
			shouldMatch: false,
		},
		{
			name: "custom type IN filter with single value - match",
			task: &MockTask{ID: "3", State: TestStateDone, Type: TestTypeSync},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpIn, []TestTaskState{TestStateDone})
				return fs
			},
			shouldMatch: true,
		},
		{
			name: "custom type IN filter with multiple values - match",
			task: &MockTask{ID: "4", State: TestStateRunning, Type: TestTypeBackup},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpIn, []TestTaskState{TestStatePending, TestStateRunning, TestStateDone})
				return fs
			},
			shouldMatch: true,
		},
		{
			name: "custom type IN filter with multiple values - no match",
			task: &MockTask{ID: "5", State: TestStateFailed, Type: TestTypeSync},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpIn, []TestTaskState{TestStatePending, TestStateRunning, TestStateDone})
				return fs
			},
			shouldMatch: false,
		},
		{
			name: "custom type NOT_IN filter - match",
			task: &MockTask{ID: "6", State: TestStateRunning, Type: TestTypeBackup},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpNotIn, []TestTaskState{TestStateFailed, TestStateDone})
				return fs
			},
			shouldMatch: true,
		},
		{
			name: "custom type NOT_IN filter - no match",
			task: &MockTask{ID: "7", State: TestStateFailed, Type: TestTypeSync},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpNotIn, []TestTaskState{TestStateFailed, TestStateDone})
				return fs
			},
			shouldMatch: false,
		},
		{
			name: "mixed custom types with AND logic - match",
			task: &MockTask{ID: "8", State: TestStateDone, Type: TestTypeBackup},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpEqual, TestStateDone)
				fs.AddFilter("Type", types.OpEqual, TestTypeBackup)
				return fs
			},
			shouldMatch: true,
		},
		{
			name: "mixed custom types with AND logic - no match",
			task: &MockTask{ID: "9", State: TestStateDone, Type: TestTypeSync},
			filters: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpEqual, TestStateDone)
				fs.AddFilter("Type", types.OpEqual, TestTypeBackup)
				return fs
			},
			shouldMatch: false,
		},
	}

	evaluator := NewMemoryFilterEvaluatorWithTypeHint(reflect.TypeOf(MockTask{}))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet := tt.filters()
			matches, err := evaluator.Evaluate(tt.task, filterSet)

			assert.NoError(t, err, "Evaluation should not error")
			assert.Equal(t, tt.shouldMatch, matches, "Match result should be correct")
		})
	}
}

// Edge case types for testing
type EdgeCaseState string

const (
	EdgeStateEmpty   EdgeCaseState = ""
	EdgeStateSpace   EdgeCaseState = " "
	EdgeStateSpecial EdgeCaseState = "with,comma"
)

type MockEdgeObject struct {
	State EdgeCaseState
}

func (obj *MockEdgeObject) GetState() EdgeCaseState { return obj.State }

// TestCustomStringTypeEdgeCases tests edge cases with custom string types
func TestCustomStringTypeEdgeCases(t *testing.T) {

	tests := []struct {
		name        string
		object      *MockEdgeObject
		filterValue interface{}
		operator    types.FilterOperator
		shouldMatch bool
		expectError bool
	}{
		{
			name:        "empty string custom type",
			object:      &MockEdgeObject{State: EdgeStateEmpty},
			filterValue: EdgeStateEmpty,
			operator:    types.OpEqual,
			shouldMatch: true,
			expectError: false,
		},
		{
			name:        "whitespace custom type",
			object:      &MockEdgeObject{State: EdgeStateSpace},
			filterValue: EdgeStateSpace,
			operator:    types.OpEqual,
			shouldMatch: true,
			expectError: false,
		},
		{
			name:        "special characters in custom type",
			object:      &MockEdgeObject{State: EdgeStateSpecial},
			filterValue: EdgeStateSpecial,
			operator:    types.OpEqual,
			shouldMatch: true,
			expectError: false,
		},
		{
			name:        "IN filter with mixed custom types",
			object:      &MockEdgeObject{State: EdgeStateSpecial},
			filterValue: []EdgeCaseState{EdgeStateEmpty, EdgeStateSpace, EdgeStateSpecial},
			operator:    types.OpIn,
			shouldMatch: true,
			expectError: false,
		},
	}

	evaluator := NewMemoryFilterEvaluatorWithTypeHint(reflect.TypeOf(MockEdgeObject{}))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet := NewFilterSet()
			filterSet.AddFilter("State", tt.operator, tt.filterValue)

			matches, err := evaluator.Evaluate(tt.object, filterSet)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.shouldMatch, matches)
			}
		})
	}
}

// Real-world scenario types
type RealTaskState string

const (
	RealTaskStatePending RealTaskState = "pending"
	RealTaskStateRunning RealTaskState = "running"
	RealTaskStateDone    RealTaskState = "done"
	RealTaskStateFailed  RealTaskState = "failed"
)

// RealTask simulates a task object like what the API would filter
type RealTask struct {
	ID               string
	Type             string
	State            RealTaskState
	CreationDatetime time.Time
}

// GetID returns the task ID
func (task *RealTask) GetID() string { return task.ID }

// GetType returns the task type
func (task *RealTask) GetType() string { return task.Type }

// GetState returns the task state
func (task *RealTask) GetState() RealTaskState { return task.State }

// GetCreationDatetime returns the creation time
func (task *RealTask) GetCreationDatetime() time.Time { return task.CreationDatetime }

// TestRealWorldTaskStateScenario tests a realistic scenario using task-like objects
func TestRealWorldTaskStateScenario(t *testing.T) {

	// Create test tasks
	now := timeutil.Now()
	tasks := []*RealTask{
		{ID: "1", Type: "public_dns_sync", State: RealTaskStateDone, CreationDatetime: now.Add(-1 * time.Hour)},
		{ID: "2", Type: "public_dns_sync", State: RealTaskStateRunning, CreationDatetime: now.Add(-30 * time.Minute)},
		{ID: "3", Type: "backup", State: RealTaskStateDone, CreationDatetime: now.Add(-2 * time.Hour)},
		{ID: "4", Type: "backup", State: RealTaskStateFailed, CreationDatetime: now.Add(-15 * time.Minute)},
	}

	tests := []struct {
		name            string
		filterSetup     func() *types.FilterSet
		expectedMatches []string // Task IDs that should match
	}{
		{
			name: "filter by type only",
			filterSetup: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("Type", types.OpEqual, "public_dns_sync")
				return fs
			},
			expectedMatches: []string{"1", "2"},
		},
		{
			name: "filter by state only - custom type",
			filterSetup: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpEqual, RealTaskStateDone)
				return fs
			},
			expectedMatches: []string{"1", "3"},
		},
		{
			name: "filter by type AND state - mixed types",
			filterSetup: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("Type", types.OpEqual, "public_dns_sync")
				fs.AddFilter("State", types.OpEqual, RealTaskStateDone)
				return fs
			},
			expectedMatches: []string{"1"},
		},
		{
			name: "filter by state IN with custom type slice",
			filterSetup: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpIn, []RealTaskState{RealTaskStateDone, RealTaskStateRunning})
				return fs
			},
			expectedMatches: []string{"1", "2", "3"},
		},
		{
			name: "filter by state NOT_IN with custom type slice",
			filterSetup: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("State", types.OpNotIn, []RealTaskState{RealTaskStateFailed})
				return fs
			},
			expectedMatches: []string{"1", "2", "3"},
		},
		{
			name: "complex filter - type AND state IN",
			filterSetup: func() *types.FilterSet {
				fs := NewFilterSet()
				fs.AddFilter("Type", types.OpEqual, "backup")
				fs.AddFilter("State", types.OpIn, []RealTaskState{RealTaskStateDone, RealTaskStateRunning})
				return fs
			},
			expectedMatches: []string{"3"},
		},
	}

	evaluator := NewMemoryFilterEvaluatorWithTypeHint(reflect.TypeOf(RealTask{}))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet := tt.filterSetup()
			var matchedIDs []string

			for _, task := range tasks {
				matches, err := evaluator.Evaluate(task, filterSet)
				assert.NoError(t, err, "Evaluation should not error for task %s", task.ID)

				if matches {
					matchedIDs = append(matchedIDs, task.ID)
				}
			}

			assert.Equal(t, tt.expectedMatches, matchedIDs, "Matched task IDs should be correct")
		})
	}
}

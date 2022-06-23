package jira

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// BoardService handles Agile Boards for the Jira instance / API.
//
// Jira API docs: https://docs.atlassian.com/jira-software/REST/server/
type BoardService struct {
	client *Client
}

// BoardsList reflects a list of agile boards
type BoardsList struct {
	MaxResults int     `json:"maxResults" structs:"maxResults"`
	StartAt    int     `json:"startAt" structs:"startAt"`
	Total      int     `json:"total" structs:"total"`
	IsLast     bool    `json:"isLast" structs:"isLast"`
	Values     []Board `json:"values" structs:"values"`
}

// Board represents a Jira agile board
type Board struct {
	ID       int    `json:"id,omitempty" structs:"id,omitempty"`
	Self     string `json:"self,omitempty" structs:"self,omitempty"`
	Name     string `json:"name,omitempty" structs:"name,omitemtpy"`
	Type     string `json:"type,omitempty" structs:"type,omitempty"`
	FilterID int    `json:"filterId,omitempty" structs:"filterId,omitempty"`
}

// BoardListOptions specifies the optional parameters to the BoardService.GetList
type BoardListOptions struct {
	// BoardType filters results to boards of the specified type.
	// Valid values: scrum, kanban.
	BoardType string `url:"type,omitempty"`
	// Name filters results to boards that match or partially match the specified name.
	Name string `url:"name,omitempty"`
	// ProjectKeyOrID filters results to boards that are relevant to a project.
	// Relevance meaning that the JQL filter defined in board contains a reference to a project.
	ProjectKeyOrID string `url:"projectKeyOrId,omitempty"`

	SearchOptions
}

// GetAllSprintsOptions specifies the optional parameters to the BoardService.GetList
type GetAllSprintsOptions struct {
	// State filters results to sprints in the specified states, comma-separate list
	State string `url:"state,omitempty"`

	SearchOptions
}

// SprintsList reflects a list of agile sprints
type SprintsList struct {
	MaxResults int      `json:"maxResults" structs:"maxResults"`
	StartAt    int      `json:"startAt" structs:"startAt"`
	Total      int      `json:"total" structs:"total"`
	IsLast     bool     `json:"isLast" structs:"isLast"`
	Values     []Sprint `json:"values" structs:"values"`
}

// Sprint represents a sprint on Jira agile board
type Sprint struct {
	ID            int        `json:"id" structs:"id"`
	Name          string     `json:"name" structs:"name"`
	CompleteDate  *time.Time `json:"completeDate" structs:"completeDate"`
	EndDate       *time.Time `json:"endDate" structs:"endDate"`
	StartDate     *time.Time `json:"startDate" structs:"startDate"`
	OriginBoardID int        `json:"originBoardId" structs:"originBoardId"`
	Self          string     `json:"self" structs:"self"`
	State         string     `json:"state" structs:"state"`
}

// BoardConfiguration represents a boardConfiguration of a jira board
type BoardConfiguration struct {
	ID           int                            `json:"id"`
	Name         string                         `json:"name"`
	Self         string                         `json:"self"`
	Location     BoardConfigurationLocation     `json:"location"`
	Filter       BoardConfigurationFilter       `json:"filter"`
	SubQuery     BoardConfigurationSubQuery     `json:"subQuery"`
	ColumnConfig BoardConfigurationColumnConfig `json:"columnConfig"`
}

// BoardConfigurationFilter reference to the filter used by the given board.
type BoardConfigurationFilter struct {
	ID   string `json:"id"`
	Self string `json:"self"`
}

// BoardConfigurationSubQuery  (Kanban only) - JQL subquery used by the given board.
type BoardConfigurationSubQuery struct {
	Query string `json:"query"`
}

// BoardConfigurationLocation reference to the container that the board is located in
type BoardConfigurationLocation struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	ID   string `json:"id"`
	Self string `json:"self"`
	Name string `json:"name"`
}

// BoardConfigurationColumnConfig lists the columns for a given board in the order defined in the column configuration
// with constrainttype (none, issueCount, issueCountExclSubs)
type BoardConfigurationColumnConfig struct {
	Columns        []BoardConfigurationColumn `json:"columns"`
	ConstraintType string                     `json:"constraintType"`
}

// BoardConfigurationColumn lists the name of the board with the statuses that maps to a particular column
type BoardConfigurationColumn struct {
	Name   string                           `json:"name"`
	Status []BoardConfigurationColumnStatus `json:"statuses"`
}

// BoardConfigurationColumnStatus represents a status in the column configuration
type BoardConfigurationColumnStatus struct {
	ID   string `json:"id"`
	Self string `json:"self"`
}

// GetAllBoardsWithContext will returns all boards. This only includes boards that the user has permission to view.
//
// Jira API docs: https://docs.atlassian.com/jira-software/REST/cloud/#agile/1.0/board-getAllBoards
func (s *BoardService) GetAllBoardsWithContext(ctx context.Context, opt *BoardListOptions) (*BoardsList, *Response, error) {
	apiEndpoint := "rest/agile/1.0/board"
	url, err := addOptions(apiEndpoint, opt)
	if err != nil {
		return nil, nil, err
	}
	req, err := s.client.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	boards := new(BoardsList)
	resp, err := s.client.Do(req, boards)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return boards, resp, err
}

// GetAllBoards wraps GetAllBoardsWithContext using the background context.
func (s *BoardService) GetAllBoards(opt *BoardListOptions) (*BoardsList, *Response, error) {
	return s.GetAllBoardsWithContext(context.Background(), opt)
}

// GetBoardWithContext will returns the board for the given boardID.
// This board will only be returned if the user has permission to view it.
//
// Jira API docs: https://docs.atlassian.com/jira-software/REST/cloud/#agile/1.0/board-getBoard
func (s *BoardService) GetBoardWithContext(ctx context.Context, boardID int) (*Board, *Response, error) {
	apiEndpoint := fmt.Sprintf("rest/agile/1.0/board/%v", boardID)
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	board := new(Board)
	resp, err := s.client.Do(req, board)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return board, resp, nil
}

// GetBoard wraps GetBoardWithContext using the background context.
func (s *BoardService) GetBoard(boardID int) (*Board, *Response, error) {
	return s.GetBoardWithContext(context.Background(), boardID)
}

// CreateBoardWithContext creates a new board. Board name, type and filter Id is required.
// name - Must be less than 255 characters.
// type - Valid values: scrum, kanban
// filterId - Id of a filter that the user has permissions to view.
// Note, if the user does not have the 'Create shared objects' permission and tries to create a shared board, a private
// board will be created instead (remember that board sharing depends on the filter sharing).
//
// Jira API docs: https://docs.atlassian.com/jira-software/REST/cloud/#agile/1.0/board-createBoard
func (s *BoardService) CreateBoardWithContext(ctx context.Context, board *Board) (*Board, *Response, error) {
	apiEndpoint := "rest/agile/1.0/board"
	req, err := s.client.NewRequestWithContext(ctx, "POST", apiEndpoint, board)
	if err != nil {
		return nil, nil, err
	}

	responseBoard := new(Board)
	resp, err := s.client.Do(req, responseBoard)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return responseBoard, resp, nil
}

// CreateBoard wraps CreateBoardWithContext using the background context.
func (s *BoardService) CreateBoard(board *Board) (*Board, *Response, error) {
	return s.CreateBoardWithContext(context.Background(), board)
}

// DeleteBoardWithContext will delete an agile board.
//
// Jira API docs: https://docs.atlassian.com/jira-software/REST/cloud/#agile/1.0/board-deleteBoard
func (s *BoardService) DeleteBoardWithContext(ctx context.Context, boardID int) (*Board, *Response, error) {
	apiEndpoint := fmt.Sprintf("rest/agile/1.0/board/%v", boardID)
	req, err := s.client.NewRequestWithContext(ctx, "DELETE", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		err = NewJiraError(resp, err)
	}
	return nil, resp, err
}

// DeleteBoard wraps DeleteBoardWithContext using the background context.
func (s *BoardService) DeleteBoard(boardID int) (*Board, *Response, error) {
	return s.DeleteBoardWithContext(context.Background(), boardID)
}

// GetAllSprintsWithContext will return all sprints from a board, for a given board Id.
// This only includes sprints that the user has permission to view.
//
// Jira API docs: https://docs.atlassian.com/jira-software/REST/cloud/#agile/1.0/board/{boardId}/sprint
func (s *BoardService) GetAllSprintsWithContext(ctx context.Context, boardID string) ([]Sprint, *Response, error) {
	id, err := strconv.Atoi(boardID)
	if err != nil {
		return nil, nil, err
	}

	result, response, err := s.GetAllSprintsWithOptions(id, &GetAllSprintsOptions{})
	if err != nil {
		return nil, nil, err
	}

	return result.Values, response, nil
}

// GetAllSprints wraps GetAllSprintsWithContext using the background context.
func (s *BoardService) GetAllSprints(boardID string) ([]Sprint, *Response, error) {
	return s.GetAllSprintsWithContext(context.Background(), boardID)
}

// GetAllSprintsWithOptionsWithContext will return sprints from a board, for a given board Id and filtering options
// This only includes sprints that the user has permission to view.
//
// Jira API docs: https://docs.atlassian.com/jira-software/REST/cloud/#agile/1.0/board/{boardId}/sprint
func (s *BoardService) GetAllSprintsWithOptionsWithContext(ctx context.Context, boardID int, options *GetAllSprintsOptions) (*SprintsList, *Response, error) {
	apiEndpoint := fmt.Sprintf("rest/agile/1.0/board/%d/sprint", boardID)
	url, err := addOptions(apiEndpoint, options)
	if err != nil {
		return nil, nil, err
	}
	req, err := s.client.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	result := new(SprintsList)
	resp, err := s.client.Do(req, result)
	if err != nil {
		err = NewJiraError(resp, err)
	}

	return result, resp, err
}

// GetAllSprintsWithOptions wraps GetAllSprintsWithOptionsWithContext using the background context.
func (s *BoardService) GetAllSprintsWithOptions(boardID int, options *GetAllSprintsOptions) (*SprintsList, *Response, error) {
	return s.GetAllSprintsWithOptionsWithContext(context.Background(), boardID, options)
}

// GetBoardConfigurationWithContext will return a board configuration for a given board Id
// Jira API docs:https://developer.atlassian.com/cloud/jira/software/rest/#api-rest-agile-1-0-board-boardId-configuration-get
func (s *BoardService) GetBoardConfigurationWithContext(ctx context.Context, boardID int) (*BoardConfiguration, *Response, error) {
	apiEndpoint := fmt.Sprintf("rest/agile/1.0/board/%d/configuration", boardID)

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)

	if err != nil {
		return nil, nil, err
	}

	result := new(BoardConfiguration)
	resp, err := s.client.Do(req, result)
	if err != nil {
		err = NewJiraError(resp, err)
	}

	return result, resp, err

}

// GetBoardConfiguration wraps GetBoardConfigurationWithContext using the background context.
func (s *BoardService) GetBoardConfiguration(boardID int) (*BoardConfiguration, *Response, error) {
	return s.GetBoardConfigurationWithContext(context.Background(), boardID)
}

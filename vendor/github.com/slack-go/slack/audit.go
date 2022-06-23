package slack

import (
	"context"
	"net/url"
	"strconv"
)

type AuditLogResponse struct {
	Entries []AuditEntry `json:"entries"`
	SlackResponse
}

type AuditEntry struct {
	ID         string `json:"id"`
	DateCreate int    `json:"date_create"`
	Action     string `json:"action"`
	Actor      struct {
		Type string    `json:"type"`
		User AuditUser `json:"user"`
	} `json:"actor"`
	Entity struct {
		Type string `json:"type"`
		// Only one of the below will be completed, based on the value of Type a user, a channel, a file, an app, a workspace, or an enterprise
		User       AuditUser       `json:"user"`
		Channel    AuditChannel    `json:"channel"`
		File       AuditFile       `json:"file"`
		App        AuditApp        `json:"app"`
		Workspace  AuditWorkspace  `json:"workspace"`
		Enterprise AuditEnterprise `json:"enterprise"`
	} `json:"entity"`
	Context struct {
		Location struct {
			Type   string `json:"type"`
			ID     string `json:"id"`
			Name   string `json:"name"`
			Domain string `json:"domain"`
		} `json:"location"`
		UA        string `json:"ua"`
		IPAddress string `json:"ip_address"`
	} `json:"context"`
}

type AuditUser struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
	Team  string `json:"team"`
}

type AuditChannel struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Privacy     string `json:"privacy"`
	IsShared    bool   `json:"is_shared"`
	IsOrgShared bool   `json:"is_org_shared"`
}

type AuditFile struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Filetype string `json:"filetype"`
	Title    string `json:"title"`
}

type AuditApp struct {
	ID                  string   `json:"id"`
	Name                string   `json:"name"`
	IsDistributed       bool     `json:"is_distributed"`
	IsDirectoryApproved bool     `json:"is_directory_approved"`
	IsWorkflowApp       bool     `json:"is_workflow_app"`
	Scopes              []string `json:"scopes"`
}

type AuditWorkspace struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Domain string `json:"domain"`
}

type AuditEnterprise struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Domain string `json:"domain"`
}

// AuditLogParameters contains all the parameters necessary (including the optional ones) for a GetAuditLogs() request
type AuditLogParameters struct {
	Limit  int
	Cursor string
	Latest int
	Oldest int
	Action string
	Actor  string
	Entity string
}

func (api *Client) auditLogsRequest(ctx context.Context, path string, values url.Values) (*AuditLogResponse, error) {
	response := &AuditLogResponse{}
	err := api.getMethod(ctx, path, api.token, values, response)
	if err != nil {
		return nil, err
	}
	return response, response.Err()
}

// GetAuditLogs retrieves a page of audit entires according to the parameters given
func (api *Client) GetAuditLogs(params AuditLogParameters) (entries []AuditEntry, nextCursor string, err error) {
	return api.GetAuditLogsContext(context.Background(), params)
}

// GetAuditLogsContext retrieves a page of audit entries according to the parameters given with a custom context
func (api *Client) GetAuditLogsContext(ctx context.Context, params AuditLogParameters) (entries []AuditEntry, nextCursor string, err error) {
	values := url.Values{}
	if params.Limit != 0 {
		values.Add("limit", strconv.Itoa(params.Limit))
	}
	if params.Oldest != 0 {
		values.Add("oldest", strconv.Itoa(params.Oldest))
	}
	if params.Latest != 0 {
		values.Add("latest", strconv.Itoa(params.Latest))
	}
	if params.Cursor != "" {
		values.Add("cursor", params.Cursor)
	}
	if params.Action != "" {
		values.Add("action", params.Action)
	}
	if params.Actor != "" {
		values.Add("actor", params.Actor)
	}
	if params.Entity != "" {
		values.Add("entity", params.Entity)
	}

	response, err := api.auditLogsRequest(ctx, "audit/v1/logs", values)
	if err != nil {
		return nil, "", err
	}
	return response.Entries, response.ResponseMetadata.Cursor, response.Err()
}

package slack

import (
	"context"
	"net/url"
	"strconv"
)

const (
	DEFAULT_LOGINS_COUNT = 100
	DEFAULT_LOGINS_PAGE  = 1
)

type TeamResponse struct {
	Team TeamInfo `json:"team"`
	SlackResponse
}

type TeamInfo struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Domain      string                 `json:"domain"`
	EmailDomain string                 `json:"email_domain"`
	Icon        map[string]interface{} `json:"icon"`
}

type LoginResponse struct {
	Logins []Login `json:"logins"`
	Paging `json:"paging"`
	SlackResponse
}

type Login struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	DateFirst int    `json:"date_first"`
	DateLast  int    `json:"date_last"`
	Count     int    `json:"count"`
	IP        string `json:"ip"`
	UserAgent string `json:"user_agent"`
	ISP       string `json:"isp"`
	Country   string `json:"country"`
	Region    string `json:"region"`
}

type BillableInfoResponse struct {
	BillableInfo map[string]BillingActive `json:"billable_info"`
	SlackResponse
}

type BillingActive struct {
	BillingActive bool `json:"billing_active"`
}

// AccessLogParameters contains all the parameters necessary (including the optional ones) for a GetAccessLogs() request
type AccessLogParameters struct {
	Count int
	Page  int
}

// NewAccessLogParameters provides an instance of AccessLogParameters with all the sane default values set
func NewAccessLogParameters() AccessLogParameters {
	return AccessLogParameters{
		Count: DEFAULT_LOGINS_COUNT,
		Page:  DEFAULT_LOGINS_PAGE,
	}
}

func (api *Client) teamRequest(ctx context.Context, path string, values url.Values) (*TeamResponse, error) {
	response := &TeamResponse{}
	err := api.postMethod(ctx, path, values, response)
	if err != nil {
		return nil, err
	}

	return response, response.Err()
}

func (api *Client) billableInfoRequest(ctx context.Context, path string, values url.Values) (map[string]BillingActive, error) {
	response := &BillableInfoResponse{}
	err := api.postMethod(ctx, path, values, response)
	if err != nil {
		return nil, err
	}

	return response.BillableInfo, response.Err()
}

func (api *Client) accessLogsRequest(ctx context.Context, path string, values url.Values) (*LoginResponse, error) {
	response := &LoginResponse{}
	err := api.postMethod(ctx, path, values, response)
	if err != nil {
		return nil, err
	}
	return response, response.Err()
}

// GetTeamInfo gets the Team Information of the user
func (api *Client) GetTeamInfo() (*TeamInfo, error) {
	return api.GetTeamInfoContext(context.Background())
}

// GetTeamInfoContext gets the Team Information of the user with a custom context
func (api *Client) GetTeamInfoContext(ctx context.Context) (*TeamInfo, error) {
	values := url.Values{
		"token": {api.token},
	}

	response, err := api.teamRequest(ctx, "team.info", values)
	if err != nil {
		return nil, err
	}
	return &response.Team, nil
}

// GetAccessLogs retrieves a page of logins according to the parameters given
func (api *Client) GetAccessLogs(params AccessLogParameters) ([]Login, *Paging, error) {
	return api.GetAccessLogsContext(context.Background(), params)
}

// GetAccessLogsContext retrieves a page of logins according to the parameters given with a custom context
func (api *Client) GetAccessLogsContext(ctx context.Context, params AccessLogParameters) ([]Login, *Paging, error) {
	values := url.Values{
		"token": {api.token},
	}
	if params.Count != DEFAULT_LOGINS_COUNT {
		values.Add("count", strconv.Itoa(params.Count))
	}
	if params.Page != DEFAULT_LOGINS_PAGE {
		values.Add("page", strconv.Itoa(params.Page))
	}

	response, err := api.accessLogsRequest(ctx, "team.accessLogs", values)
	if err != nil {
		return nil, nil, err
	}
	return response.Logins, &response.Paging, nil
}

// GetBillableInfo ...
func (api *Client) GetBillableInfo(user string) (map[string]BillingActive, error) {
	return api.GetBillableInfoContext(context.Background(), user)
}

// GetBillableInfoContext ...
func (api *Client) GetBillableInfoContext(ctx context.Context, user string) (map[string]BillingActive, error) {
	values := url.Values{
		"token": {api.token},
		"user":  {user},
	}

	return api.billableInfoRequest(ctx, "team.billableInfo", values)
}

// GetBillableInfoForTeam returns the billing_active status of all users on the team.
func (api *Client) GetBillableInfoForTeam() (map[string]BillingActive, error) {
	return api.GetBillableInfoForTeamContext(context.Background())
}

// GetBillableInfoForTeamContext returns the billing_active status of all users on the team with a custom context
func (api *Client) GetBillableInfoForTeamContext(ctx context.Context) (map[string]BillingActive, error) {
	values := url.Values{
		"token": {api.token},
	}

	return api.billableInfoRequest(ctx, "team.billableInfo", values)
}

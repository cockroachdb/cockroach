package slack

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DEFAULT_USER_PHOTO_CROP_X = -1
	DEFAULT_USER_PHOTO_CROP_Y = -1
	DEFAULT_USER_PHOTO_CROP_W = -1
)

// UserProfile contains all the information details of a given user
type UserProfile struct {
	FirstName             string                  `json:"first_name"`
	LastName              string                  `json:"last_name"`
	RealName              string                  `json:"real_name"`
	RealNameNormalized    string                  `json:"real_name_normalized"`
	DisplayName           string                  `json:"display_name"`
	DisplayNameNormalized string                  `json:"display_name_normalized"`
	Email                 string                  `json:"email"`
	Skype                 string                  `json:"skype"`
	Phone                 string                  `json:"phone"`
	Image24               string                  `json:"image_24"`
	Image32               string                  `json:"image_32"`
	Image48               string                  `json:"image_48"`
	Image72               string                  `json:"image_72"`
	Image192              string                  `json:"image_192"`
	Image512              string                  `json:"image_512"`
	ImageOriginal         string                  `json:"image_original"`
	Title                 string                  `json:"title"`
	BotID                 string                  `json:"bot_id,omitempty"`
	ApiAppID              string                  `json:"api_app_id,omitempty"`
	StatusText            string                  `json:"status_text,omitempty"`
	StatusEmoji           string                  `json:"status_emoji,omitempty"`
	StatusExpiration      int                     `json:"status_expiration"`
	Team                  string                  `json:"team"`
	Fields                UserProfileCustomFields `json:"fields"`
}

// UserProfileCustomFields represents user profile's custom fields.
// Slack API's response data type is inconsistent so we use the struct.
// For detail, please see below.
// https://github.com/slack-go/slack/pull/298#discussion_r185159233
type UserProfileCustomFields struct {
	fields map[string]UserProfileCustomField
}

// UnmarshalJSON is the implementation of the json.Unmarshaler interface.
func (fields *UserProfileCustomFields) UnmarshalJSON(b []byte) error {
	// https://github.com/slack-go/slack/pull/298#discussion_r185159233
	if string(b) == "[]" {
		return nil
	}
	return json.Unmarshal(b, &fields.fields)
}

// MarshalJSON is the implementation of the json.Marshaler interface.
func (fields UserProfileCustomFields) MarshalJSON() ([]byte, error) {
	if len(fields.fields) == 0 {
		return []byte("[]"), nil
	}
	return json.Marshal(fields.fields)
}

// ToMap returns a map of custom fields.
func (fields *UserProfileCustomFields) ToMap() map[string]UserProfileCustomField {
	return fields.fields
}

// Len returns the number of custom fields.
func (fields *UserProfileCustomFields) Len() int {
	return len(fields.fields)
}

// SetMap sets a map of custom fields.
func (fields *UserProfileCustomFields) SetMap(m map[string]UserProfileCustomField) {
	fields.fields = m
}

// FieldsMap returns a map of custom fields.
func (profile *UserProfile) FieldsMap() map[string]UserProfileCustomField {
	return profile.Fields.ToMap()
}

// SetFieldsMap sets a map of custom fields.
func (profile *UserProfile) SetFieldsMap(m map[string]UserProfileCustomField) {
	profile.Fields.SetMap(m)
}

// UserProfileCustomField represents a custom user profile field
type UserProfileCustomField struct {
	Value string `json:"value"`
	Alt   string `json:"alt"`
	Label string `json:"label"`
}

// User contains all the information of a user
type User struct {
	ID                string         `json:"id"`
	TeamID            string         `json:"team_id"`
	Name              string         `json:"name"`
	Deleted           bool           `json:"deleted"`
	Color             string         `json:"color"`
	RealName          string         `json:"real_name"`
	TZ                string         `json:"tz,omitempty"`
	TZLabel           string         `json:"tz_label"`
	TZOffset          int            `json:"tz_offset"`
	Profile           UserProfile    `json:"profile"`
	IsBot             bool           `json:"is_bot"`
	IsAdmin           bool           `json:"is_admin"`
	IsOwner           bool           `json:"is_owner"`
	IsPrimaryOwner    bool           `json:"is_primary_owner"`
	IsRestricted      bool           `json:"is_restricted"`
	IsUltraRestricted bool           `json:"is_ultra_restricted"`
	IsStranger        bool           `json:"is_stranger"`
	IsAppUser         bool           `json:"is_app_user"`
	IsInvitedUser     bool           `json:"is_invited_user"`
	Has2FA            bool           `json:"has_2fa"`
	HasFiles          bool           `json:"has_files"`
	Presence          string         `json:"presence"`
	Locale            string         `json:"locale"`
	Updated           JSONTime       `json:"updated"`
	Enterprise        EnterpriseUser `json:"enterprise_user,omitempty"`
}

// UserPresence contains details about a user online status
type UserPresence struct {
	Presence        string   `json:"presence,omitempty"`
	Online          bool     `json:"online,omitempty"`
	AutoAway        bool     `json:"auto_away,omitempty"`
	ManualAway      bool     `json:"manual_away,omitempty"`
	ConnectionCount int      `json:"connection_count,omitempty"`
	LastActivity    JSONTime `json:"last_activity,omitempty"`
}

type UserIdentityResponse struct {
	User UserIdentity `json:"user"`
	Team TeamIdentity `json:"team"`
	SlackResponse
}

type UserIdentity struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Image24  string `json:"image_24"`
	Image32  string `json:"image_32"`
	Image48  string `json:"image_48"`
	Image72  string `json:"image_72"`
	Image192 string `json:"image_192"`
	Image512 string `json:"image_512"`
}

// EnterpriseUser is present when a user is part of Slack Enterprise Grid
// https://api.slack.com/types/user#enterprise_grid_user_objects
type EnterpriseUser struct {
	ID             string   `json:"id"`
	EnterpriseID   string   `json:"enterprise_id"`
	EnterpriseName string   `json:"enterprise_name"`
	IsAdmin        bool     `json:"is_admin"`
	IsOwner        bool     `json:"is_owner"`
	Teams          []string `json:"teams"`
}

type TeamIdentity struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Domain        string `json:"domain"`
	Image34       string `json:"image_34"`
	Image44       string `json:"image_44"`
	Image68       string `json:"image_68"`
	Image88       string `json:"image_88"`
	Image102      string `json:"image_102"`
	Image132      string `json:"image_132"`
	Image230      string `json:"image_230"`
	ImageDefault  bool   `json:"image_default"`
	ImageOriginal string `json:"image_original"`
}

type userResponseFull struct {
	Members []User `json:"members,omitempty"`
	User    `json:"user,omitempty"`
	Users   []User `json:"users,omitempty"`
	UserPresence
	SlackResponse
	Metadata ResponseMetadata `json:"response_metadata"`
}

type UserSetPhotoParams struct {
	CropX int
	CropY int
	CropW int
}

func NewUserSetPhotoParams() UserSetPhotoParams {
	return UserSetPhotoParams{
		CropX: DEFAULT_USER_PHOTO_CROP_X,
		CropY: DEFAULT_USER_PHOTO_CROP_Y,
		CropW: DEFAULT_USER_PHOTO_CROP_W,
	}
}

func (api *Client) userRequest(ctx context.Context, path string, values url.Values) (*userResponseFull, error) {
	response := &userResponseFull{}
	err := api.postMethod(ctx, path, values, response)
	if err != nil {
		return nil, err
	}

	return response, response.Err()
}

// GetUserPresence will retrieve the current presence status of given user.
func (api *Client) GetUserPresence(user string) (*UserPresence, error) {
	return api.GetUserPresenceContext(context.Background(), user)
}

// GetUserPresenceContext will retrieve the current presence status of given user with a custom context.
func (api *Client) GetUserPresenceContext(ctx context.Context, user string) (*UserPresence, error) {
	values := url.Values{
		"token": {api.token},
		"user":  {user},
	}

	response, err := api.userRequest(ctx, "users.getPresence", values)
	if err != nil {
		return nil, err
	}
	return &response.UserPresence, nil
}

// GetUserInfo will retrieve the complete user information
func (api *Client) GetUserInfo(user string) (*User, error) {
	return api.GetUserInfoContext(context.Background(), user)
}

// GetUserInfoContext will retrieve the complete user information with a custom context
func (api *Client) GetUserInfoContext(ctx context.Context, user string) (*User, error) {
	values := url.Values{
		"token":          {api.token},
		"user":           {user},
		"include_locale": {strconv.FormatBool(true)},
	}

	response, err := api.userRequest(ctx, "users.info", values)
	if err != nil {
		return nil, err
	}
	return &response.User, nil
}

// GetUsersInfo will retrieve the complete multi-users information
func (api *Client) GetUsersInfo(users ...string) (*[]User, error) {
	return api.GetUsersInfoContext(context.Background(), users...)
}

// GetUsersInfoContext will retrieve the complete multi-users information with a custom context
func (api *Client) GetUsersInfoContext(ctx context.Context, users ...string) (*[]User, error) {
	values := url.Values{
		"token":          {api.token},
		"users":          {strings.Join(users, ",")},
		"include_locale": {strconv.FormatBool(true)},
	}

	response, err := api.userRequest(ctx, "users.info", values)
	if err != nil {
		return nil, err
	}
	return &response.Users, nil
}

// GetUsersOption options for the GetUsers method call.
type GetUsersOption func(*UserPagination)

// GetUsersOptionLimit limit the number of users returned
func GetUsersOptionLimit(n int) GetUsersOption {
	return func(p *UserPagination) {
		p.limit = n
	}
}

// GetUsersOptionPresence include user presence
func GetUsersOptionPresence(n bool) GetUsersOption {
	return func(p *UserPagination) {
		p.presence = n
	}
}

func newUserPagination(c *Client, options ...GetUsersOption) (up UserPagination) {
	up = UserPagination{
		c:     c,
		limit: 200, // per slack api documentation.
	}

	for _, opt := range options {
		opt(&up)
	}

	return up
}

// UserPagination allows for paginating over the users
type UserPagination struct {
	Users        []User
	limit        int
	presence     bool
	previousResp *ResponseMetadata
	c            *Client
}

// Done checks if the pagination has completed
func (UserPagination) Done(err error) bool {
	return err == errPaginationComplete
}

// Failure checks if pagination failed.
func (t UserPagination) Failure(err error) error {
	if t.Done(err) {
		return nil
	}

	return err
}

func (t UserPagination) Next(ctx context.Context) (_ UserPagination, err error) {
	var (
		resp *userResponseFull
	)

	if t.c == nil || (t.previousResp != nil && t.previousResp.Cursor == "") {
		return t, errPaginationComplete
	}

	t.previousResp = t.previousResp.initialize()

	values := url.Values{
		"limit":          {strconv.Itoa(t.limit)},
		"presence":       {strconv.FormatBool(t.presence)},
		"token":          {t.c.token},
		"cursor":         {t.previousResp.Cursor},
		"include_locale": {strconv.FormatBool(true)},
	}

	if resp, err = t.c.userRequest(ctx, "users.list", values); err != nil {
		return t, err
	}

	t.c.Debugf("GetUsersContext: got %d users; metadata %v", len(resp.Members), resp.Metadata)
	t.Users = resp.Members
	t.previousResp = &resp.Metadata

	return t, nil
}

// GetUsersPaginated fetches users in a paginated fashion, see GetUsersContext for usage.
func (api *Client) GetUsersPaginated(options ...GetUsersOption) UserPagination {
	return newUserPagination(api, options...)
}

// GetUsers returns the list of users (with their detailed information)
func (api *Client) GetUsers() ([]User, error) {
	return api.GetUsersContext(context.Background())
}

// GetUsersContext returns the list of users (with their detailed information) with a custom context
func (api *Client) GetUsersContext(ctx context.Context) (results []User, err error) {
	p := api.GetUsersPaginated()
	for err == nil {
		p, err = p.Next(ctx)
		if err == nil {
			results = append(results, p.Users...)
		} else if rateLimitedError, ok := err.(*RateLimitedError); ok {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(rateLimitedError.RetryAfter):
				err = nil
			}
		}
	}

	return results, p.Failure(err)
}

// GetUserByEmail will retrieve the complete user information by email
func (api *Client) GetUserByEmail(email string) (*User, error) {
	return api.GetUserByEmailContext(context.Background(), email)
}

// GetUserByEmailContext will retrieve the complete user information by email with a custom context
func (api *Client) GetUserByEmailContext(ctx context.Context, email string) (*User, error) {
	values := url.Values{
		"token": {api.token},
		"email": {email},
	}
	response, err := api.userRequest(ctx, "users.lookupByEmail", values)
	if err != nil {
		return nil, err
	}
	return &response.User, nil
}

// SetUserAsActive marks the currently authenticated user as active
func (api *Client) SetUserAsActive() error {
	return api.SetUserAsActiveContext(context.Background())
}

// SetUserAsActiveContext marks the currently authenticated user as active with a custom context
func (api *Client) SetUserAsActiveContext(ctx context.Context) (err error) {
	values := url.Values{
		"token": {api.token},
	}

	_, err = api.userRequest(ctx, "users.setActive", values)
	return err
}

// SetUserPresence changes the currently authenticated user presence
func (api *Client) SetUserPresence(presence string) error {
	return api.SetUserPresenceContext(context.Background(), presence)
}

// SetUserPresenceContext changes the currently authenticated user presence with a custom context
func (api *Client) SetUserPresenceContext(ctx context.Context, presence string) error {
	values := url.Values{
		"token":    {api.token},
		"presence": {presence},
	}

	_, err := api.userRequest(ctx, "users.setPresence", values)
	return err
}

// GetUserIdentity will retrieve user info available per identity scopes
func (api *Client) GetUserIdentity() (*UserIdentityResponse, error) {
	return api.GetUserIdentityContext(context.Background())
}

// GetUserIdentityContext will retrieve user info available per identity scopes with a custom context
func (api *Client) GetUserIdentityContext(ctx context.Context) (response *UserIdentityResponse, err error) {
	values := url.Values{
		"token": {api.token},
	}
	response = &UserIdentityResponse{}

	err = api.postMethod(ctx, "users.identity", values, response)
	if err != nil {
		return nil, err
	}

	if err := response.Err(); err != nil {
		return nil, err
	}

	return response, nil
}

// SetUserPhoto changes the currently authenticated user's profile image
func (api *Client) SetUserPhoto(image string, params UserSetPhotoParams) error {
	return api.SetUserPhotoContext(context.Background(), image, params)
}

// SetUserPhotoContext changes the currently authenticated user's profile image using a custom context
func (api *Client) SetUserPhotoContext(ctx context.Context, image string, params UserSetPhotoParams) (err error) {
	response := &SlackResponse{}
	values := url.Values{
		"token": {api.token},
	}
	if params.CropX != DEFAULT_USER_PHOTO_CROP_X {
		values.Add("crop_x", strconv.Itoa(params.CropX))
	}
	if params.CropY != DEFAULT_USER_PHOTO_CROP_Y {
		values.Add("crop_y", strconv.Itoa(params.CropX))
	}
	if params.CropW != DEFAULT_USER_PHOTO_CROP_W {
		values.Add("crop_w", strconv.Itoa(params.CropW))
	}

	err = postLocalWithMultipartResponse(ctx, api.httpclient, api.endpoint+"users.setPhoto", image, "image", api.token, values, response, api)
	if err != nil {
		return err
	}

	return response.Err()
}

// DeleteUserPhoto deletes the current authenticated user's profile image
func (api *Client) DeleteUserPhoto() error {
	return api.DeleteUserPhotoContext(context.Background())
}

// DeleteUserPhotoContext deletes the current authenticated user's profile image with a custom context
func (api *Client) DeleteUserPhotoContext(ctx context.Context) (err error) {
	response := &SlackResponse{}
	values := url.Values{
		"token": {api.token},
	}

	err = api.postMethod(ctx, "users.deletePhoto", values, response)
	if err != nil {
		return err
	}

	return response.Err()
}

// SetUserRealName changes the currently authenticated user's realName
//
// For more information see SetUserRealNameContextWithUser
func (api *Client) SetUserRealName(realName string) error {
	return api.SetUserRealNameContextWithUser(context.Background(), "", realName)
}

// SetUserRealNameContextWithUser will set a real name for the provided user with a custom context
func (api *Client) SetUserRealNameContextWithUser(ctx context.Context, user, realName string) error {
	profile, err := json.Marshal(
		&struct {
			RealName string `json:"real_name"`
		}{
			RealName: realName,
		},
	)

	if err != nil {
		return err
	}

	values := url.Values{
		"token":   {api.token},
		"profile": {string(profile)},
	}

	// optional field. It should not be set if empty
	if user != "" {
		values["user"] = []string{user}
	}

	response := &userResponseFull{}
	if err = api.postMethod(ctx, "users.profile.set", values, response); err != nil {
		return err
	}

	return response.Err()
}

// SetUserCustomStatus will set a custom status and emoji for the currently
// authenticated user. If statusEmoji is "" and statusText is not, the Slack API
// will automatically set it to ":speech_balloon:". Otherwise, if both are ""
// the Slack API will unset the custom status/emoji. If statusExpiration is set to 0
// the status will not expire.
func (api *Client) SetUserCustomStatus(statusText, statusEmoji string, statusExpiration int64) error {
	return api.SetUserCustomStatusContextWithUser(context.Background(), "", statusText, statusEmoji, statusExpiration)
}

// SetUserCustomStatusContext will set a custom status and emoji for the currently authenticated user with a custom context
//
// For more information see SetUserCustomStatus
func (api *Client) SetUserCustomStatusContext(ctx context.Context, statusText, statusEmoji string, statusExpiration int64) error {
	return api.SetUserCustomStatusContextWithUser(ctx, "", statusText, statusEmoji, statusExpiration)
}

// SetUserCustomStatusWithUser will set a custom status and emoji for the provided user.
//
// For more information see SetUserCustomStatus
func (api *Client) SetUserCustomStatusWithUser(user, statusText, statusEmoji string, statusExpiration int64) error {
	return api.SetUserCustomStatusContextWithUser(context.Background(), user, statusText, statusEmoji, statusExpiration)
}

// SetUserCustomStatusContextWithUser will set a custom status and emoji for the provided user with a custom context
//
// For more information see SetUserCustomStatus
func (api *Client) SetUserCustomStatusContextWithUser(ctx context.Context, user, statusText, statusEmoji string, statusExpiration int64) error {
	// XXX(theckman): this anonymous struct is for making requests to the Slack
	// API for setting and unsetting a User's Custom Status/Emoji. To change
	// these values we must provide a JSON document as the profile POST field.
	//
	// We use an anonymous struct over UserProfile because to unset the values
	// on the User's profile we cannot use the `json:"omitempty"` tag. This is
	// because an empty string ("") is what's used to unset the values. Check
	// out the API docs for more details:
	//
	// - https://api.slack.com/docs/presence-and-status#custom_status
	profile, err := json.Marshal(
		&struct {
			StatusText       string `json:"status_text"`
			StatusEmoji      string `json:"status_emoji"`
			StatusExpiration int64  `json:"status_expiration"`
		}{
			StatusText:       statusText,
			StatusEmoji:      statusEmoji,
			StatusExpiration: statusExpiration,
		},
	)

	if err != nil {
		return err
	}

	values := url.Values{
		"token":   {api.token},
		"profile": {string(profile)},
	}

	// optional field. It should not be set if empty
	if user != "" {
		values["user"] = []string{user}
	}

	response := &userResponseFull{}
	if err = api.postMethod(ctx, "users.profile.set", values, response); err != nil {
		return err
	}

	return response.Err()
}

// UnsetUserCustomStatus removes the custom status message for the currently
// authenticated user. This is a convenience method that wraps (*Client).SetUserCustomStatus().
func (api *Client) UnsetUserCustomStatus() error {
	return api.UnsetUserCustomStatusContext(context.Background())
}

// UnsetUserCustomStatusContext removes the custom status message for the currently authenticated user
// with a custom context. This is a convenience method that wraps (*Client).SetUserCustomStatus().
func (api *Client) UnsetUserCustomStatusContext(ctx context.Context) error {
	return api.SetUserCustomStatusContext(ctx, "", "", 0)
}

// GetUserProfileParameters are the parameters required to get user profile
type GetUserProfileParameters struct {
	UserID        string
	IncludeLabels bool
}

// GetUserProfile retrieves a user's profile information.
func (api *Client) GetUserProfile(params *GetUserProfileParameters) (*UserProfile, error) {
	return api.GetUserProfileContext(context.Background(), params)
}

type getUserProfileResponse struct {
	SlackResponse
	Profile *UserProfile `json:"profile"`
}

// GetUserProfileContext retrieves a user's profile information with a context.
func (api *Client) GetUserProfileContext(ctx context.Context, params *GetUserProfileParameters) (*UserProfile, error) {
	values := url.Values{"token": {api.token}}

	if params.UserID != "" {
		values.Add("user", params.UserID)
	}
	if params.IncludeLabels {
		values.Add("include_labels", "true")
	}
	resp := &getUserProfileResponse{}

	err := api.postMethod(ctx, "users.profile.get", values, &resp)
	if err != nil {
		return nil, err
	}

	if err := resp.Err(); err != nil {
		return nil, err
	}

	return resp.Profile, nil
}

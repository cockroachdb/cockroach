package teamcity

type Changes []Change

type Change struct {
	Username string
	ID       int
	Date     string
	HREF     string
	Version  string
	WebURL   string
}

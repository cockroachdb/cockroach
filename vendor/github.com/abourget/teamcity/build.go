package teamcity

import "fmt"

// Build represents a TeamCity build, along with its metadata.
type Build struct {
	ID          int64
	BuildTypeID string
	BuildType   struct {
		ID          string
		Name        string
		Description string
		ProjectName string
		ProjectID   string
		HREF        string
		WebURL      string
	}
	Triggered struct {
		Type string
		Date JSONTime
		User struct {
			Username string
		}
	}
	Changes struct {
		Change []Change
	}

	QueuedDate    JSONTime
	StartDate     JSONTime
	FinishDate    JSONTime
	Number        string
	Status        string
	StatusText    string
	State         string
	BranchName    string
	Personal      bool
	Running       bool
	Pinned        bool
	DefaultBranch bool
	HREF          string
	WebURL        string
	Agent         struct {
		ID     int64
		Name   string
		TypeID int64
		HREF   string
	}

	ProblemOccurrences struct {
		ProblemOccurrence []ProblemOccurrence
	}

	TestOccurrences struct {
		TestOccurrence []TestOccurrence
	}

	// As received from the API
	TagsInput struct {
		Tag []struct {
			Name string
		}
	} `json:"tags"`

	// Useable, filled before sending to `IncomingBuilds`
	Tags []string `json:"-"`

	// As received from the API
	PropertiesInput struct {
		Property []oneProperty `json:"property"`
	} `json:"properties"`

	// Useable, filled before sending to `IncomingBuilds`
	Properties map[string]string `json:"-"`
}

type oneProperty struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (b *Build) convertInputs() {
	b.Tags = make([]string, 0)
	for _, tag := range b.TagsInput.Tag {
		b.Tags = append(b.Tags, tag.Name)
	}

	b.Properties = make(map[string]string)
	for _, prop := range b.PropertiesInput.Property {
		b.Properties[prop.Name] = prop.Value
	}
}

func (b *Build) String() string {
	return fmt.Sprintf("Build %d, %#v state=%s", b.ID, b.ComputedState(), b.State)
}

type State int

const (
	Unknown = State(iota)
	Queued
	Started
	Finished
)

func (b *Build) ComputedState() State {
	if b.QueuedDate == "" {
		return Unknown
	}
	if b.StartDate == "" {
		return Queued
	}
	if b.FinishDate == "" {
		return Started
	}
	return Finished
}

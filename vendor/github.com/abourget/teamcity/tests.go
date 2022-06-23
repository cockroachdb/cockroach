package teamcity

type TestOccurrence struct {
	ID                    string
	Name                  string
	Status                string
	Muted                 bool
	Duration              int64
	CurrentlyMuted        bool
	CurrentlyInvestigated bool
	HREF                  string
	Details               string
}

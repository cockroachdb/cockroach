package main

import (
	"encoding/json"
	"net/url"
)

type Lockfile = map[string]LockfileEntry
type Lockfiles = map[string][]LockfileEntry

type LockfileEntry struct {
	Name         string
	Version      string
	Resolved     *url.URL
	Integrity    string
	Dependencies map[string]string
}

type IntermediateEntry struct {
	Version      string            `json:"version,omitempty"`
	Resolved     string            `json:"resolved,omitempty"`
	Integrity    string            `json:"integrity,omitempty"`
	Dependencies map[string]string `json:"dependencies,omitempty"`
}

func (lfe *LockfileEntry) UnmarshalJSON(in []byte) error {
	ie := new(IntermediateEntry)
	if err := json.Unmarshal(in, &ie); err != nil {
		return err
	}

	lfe.Version = ie.Version
	lfe.Integrity = ie.Integrity
	lfe.Dependencies = ie.Dependencies

	if ie.Resolved != "" {
		resolvedUrl, err := url.Parse(ie.Resolved)
		if err != nil {
			return err
		}
		lfe.Resolved = resolvedUrl
	}
	return nil
}

func (lfe LockfileEntry) MarshalJSON() ([]byte, error) {
	ie := new(IntermediateEntry)
	ie.Version = lfe.Version
	ie.Integrity = lfe.Integrity
	ie.Dependencies = lfe.Dependencies
	if lfe.Resolved != nil {
		ie.Resolved = lfe.Resolved.String()
	}

	return json.Marshal(ie)
}

/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package lifecycle contains all the lifecycle related data types and marshallers.
package lifecycle

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"time"
)

var errMissingStorageClass = errors.New("storage-class cannot be empty")

// AbortIncompleteMultipartUpload structure, not supported yet on MinIO
type AbortIncompleteMultipartUpload struct {
	XMLName             xml.Name       `xml:"AbortIncompleteMultipartUpload,omitempty"  json:"-"`
	DaysAfterInitiation ExpirationDays `xml:"DaysAfterInitiation,omitempty" json:"DaysAfterInitiation,omitempty"`
}

// IsDaysNull returns true if days field is null
func (n AbortIncompleteMultipartUpload) IsDaysNull() bool {
	return n.DaysAfterInitiation == ExpirationDays(0)
}

// MarshalXML if days after initiation is set to non-zero value
func (n AbortIncompleteMultipartUpload) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if n.IsDaysNull() {
		return nil
	}
	type abortIncompleteMultipartUploadWrapper AbortIncompleteMultipartUpload
	return e.EncodeElement(abortIncompleteMultipartUploadWrapper(n), start)
}

// NoncurrentVersionExpiration - Specifies when noncurrent object versions expire.
// Upon expiration, server permanently deletes the noncurrent object versions.
// Set this lifecycle configuration action on a bucket that has versioning enabled
// (or suspended) to request server delete noncurrent object versions at a
// specific period in the object's lifetime.
type NoncurrentVersionExpiration struct {
	XMLName                 xml.Name       `xml:"NoncurrentVersionExpiration" json:"-"`
	NoncurrentDays          ExpirationDays `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions int            `xml:"NewerNoncurrentVersions,omitempty"`
}

// MarshalXML if n is non-empty, i.e has a non-zero NoncurrentDays or NewerNoncurrentVersions.
func (n NoncurrentVersionExpiration) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if n.isNull() {
		return nil
	}
	type noncurrentVersionExpirationWrapper NoncurrentVersionExpiration
	return e.EncodeElement(noncurrentVersionExpirationWrapper(n), start)
}

// IsDaysNull returns true if days field is null
func (n NoncurrentVersionExpiration) IsDaysNull() bool {
	return n.NoncurrentDays == ExpirationDays(0)
}

func (n NoncurrentVersionExpiration) isNull() bool {
	return n.IsDaysNull() && n.NewerNoncurrentVersions == 0
}

// NoncurrentVersionTransition structure, set this action to request server to
// transition noncurrent object versions to different set storage classes
// at a specific period in the object's lifetime.
type NoncurrentVersionTransition struct {
	XMLName                 xml.Name       `xml:"NoncurrentVersionTransition,omitempty"  json:"-"`
	StorageClass            string         `xml:"StorageClass,omitempty" json:"StorageClass,omitempty"`
	NoncurrentDays          ExpirationDays `xml:"NoncurrentDays" json:"NoncurrentDays"`
	NewerNoncurrentVersions int            `xml:"NewerNoncurrentVersions,omitempty" json:"NewerNoncurrentVersions,omitempty"`
}

// IsDaysNull returns true if days field is null
func (n NoncurrentVersionTransition) IsDaysNull() bool {
	return n.NoncurrentDays == ExpirationDays(0)
}

// IsStorageClassEmpty returns true if storage class field is empty
func (n NoncurrentVersionTransition) IsStorageClassEmpty() bool {
	return n.StorageClass == ""
}

func (n NoncurrentVersionTransition) isNull() bool {
	return n.StorageClass == ""
}

// UnmarshalJSON implements NoncurrentVersionTransition JSONify
func (n *NoncurrentVersionTransition) UnmarshalJSON(b []byte) error {
	type noncurrentVersionTransition NoncurrentVersionTransition
	var nt noncurrentVersionTransition
	err := json.Unmarshal(b, &nt)
	if err != nil {
		return err
	}

	if nt.StorageClass == "" {
		return errMissingStorageClass
	}
	*n = NoncurrentVersionTransition(nt)
	return nil
}

// MarshalXML is extended to leave out
// <NoncurrentVersionTransition></NoncurrentVersionTransition> tags
func (n NoncurrentVersionTransition) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if n.isNull() {
		return nil
	}
	type noncurrentVersionTransitionWrapper NoncurrentVersionTransition
	return e.EncodeElement(noncurrentVersionTransitionWrapper(n), start)
}

// Tag structure key/value pair representing an object tag to apply lifecycle configuration
type Tag struct {
	XMLName xml.Name `xml:"Tag,omitempty" json:"-"`
	Key     string   `xml:"Key,omitempty" json:"Key,omitempty"`
	Value   string   `xml:"Value,omitempty" json:"Value,omitempty"`
}

// IsEmpty returns whether this tag is empty or not.
func (tag Tag) IsEmpty() bool {
	return tag.Key == ""
}

// Transition structure - transition details of lifecycle configuration
type Transition struct {
	XMLName      xml.Name       `xml:"Transition" json:"-"`
	Date         ExpirationDate `xml:"Date,omitempty" json:"Date,omitempty"`
	StorageClass string         `xml:"StorageClass,omitempty" json:"StorageClass,omitempty"`
	Days         ExpirationDays `xml:"Days" json:"Days"`
}

// UnmarshalJSON returns an error if storage-class is empty.
func (t *Transition) UnmarshalJSON(b []byte) error {
	type transition Transition
	var tr transition
	err := json.Unmarshal(b, &tr)
	if err != nil {
		return err
	}

	if tr.StorageClass == "" {
		return errMissingStorageClass
	}
	*t = Transition(tr)
	return nil
}

// MarshalJSON customizes json encoding by omitting empty values
func (t Transition) MarshalJSON() ([]byte, error) {
	if t.IsNull() {
		return nil, nil
	}
	type transition struct {
		Date         *ExpirationDate `json:"Date,omitempty"`
		StorageClass string          `json:"StorageClass,omitempty"`
		Days         *ExpirationDays `json:"Days"`
	}

	newt := transition{
		StorageClass: t.StorageClass,
	}

	if !t.IsDateNull() {
		newt.Date = &t.Date
	} else {
		newt.Days = &t.Days
	}
	return json.Marshal(newt)
}

// IsDaysNull returns true if days field is null
func (t Transition) IsDaysNull() bool {
	return t.Days == ExpirationDays(0)
}

// IsDateNull returns true if date field is null
func (t Transition) IsDateNull() bool {
	return t.Date.Time.IsZero()
}

// IsNull returns true if no storage-class is set.
func (t Transition) IsNull() bool {
	return t.StorageClass == ""
}

// MarshalXML is transition is non null
func (t Transition) MarshalXML(en *xml.Encoder, startElement xml.StartElement) error {
	if t.IsNull() {
		return nil
	}
	type transitionWrapper Transition
	return en.EncodeElement(transitionWrapper(t), startElement)
}

// And And Rule for LifecycleTag, to be used in LifecycleRuleFilter
type And struct {
	XMLName xml.Name `xml:"And" json:"-"`
	Prefix  string   `xml:"Prefix" json:"Prefix,omitempty"`
	Tags    []Tag    `xml:"Tag" json:"Tags,omitempty"`
}

// IsEmpty returns true if Tags field is null
func (a And) IsEmpty() bool {
	return len(a.Tags) == 0 && a.Prefix == ""
}

// Filter will be used in selecting rule(s) for lifecycle configuration
type Filter struct {
	XMLName xml.Name `xml:"Filter" json:"-"`
	And     And      `xml:"And,omitempty" json:"And,omitempty"`
	Prefix  string   `xml:"Prefix,omitempty" json:"Prefix,omitempty"`
	Tag     Tag      `xml:"Tag,omitempty" json:"Tag,omitempty"`
}

// IsNull returns true if all Filter fields are empty.
func (f Filter) IsNull() bool {
	return f.Tag.IsEmpty() && f.And.IsEmpty() && f.Prefix == ""
}

// MarshalJSON customizes json encoding by removing empty values.
func (f Filter) MarshalJSON() ([]byte, error) {
	type filter struct {
		And    *And   `json:"And,omitempty"`
		Prefix string `json:"Prefix,omitempty"`
		Tag    *Tag   `json:"Tag,omitempty"`
	}

	newf := filter{
		Prefix: f.Prefix,
	}
	if !f.Tag.IsEmpty() {
		newf.Tag = &f.Tag
	}
	if !f.And.IsEmpty() {
		newf.And = &f.And
	}
	return json.Marshal(newf)
}

// MarshalXML - produces the xml representation of the Filter struct
// only one of Prefix, And and Tag should be present in the output.
func (f Filter) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	switch {
	case !f.And.IsEmpty():
		if err := e.EncodeElement(f.And, xml.StartElement{Name: xml.Name{Local: "And"}}); err != nil {
			return err
		}
	case !f.Tag.IsEmpty():
		if err := e.EncodeElement(f.Tag, xml.StartElement{Name: xml.Name{Local: "Tag"}}); err != nil {
			return err
		}
	default:
		// Always print Prefix field when both And & Tag are empty
		if err := e.EncodeElement(f.Prefix, xml.StartElement{Name: xml.Name{Local: "Prefix"}}); err != nil {
			return err
		}
	}

	return e.EncodeToken(xml.EndElement{Name: start.Name})
}

// ExpirationDays is a type alias to unmarshal Days in Expiration
type ExpirationDays int

// MarshalXML encodes number of days to expire if it is non-zero and
// encodes empty string otherwise
func (eDays ExpirationDays) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if eDays == 0 {
		return nil
	}
	return e.EncodeElement(int(eDays), startElement)
}

// ExpirationDate is a embedded type containing time.Time to unmarshal
// Date in Expiration
type ExpirationDate struct {
	time.Time
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (eDate ExpirationDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if eDate.Time.IsZero() {
		return nil
	}
	return e.EncodeElement(eDate.Format(time.RFC3339), startElement)
}

// ExpireDeleteMarker represents value of ExpiredObjectDeleteMarker field in Expiration XML element.
type ExpireDeleteMarker bool

// MarshalXML encodes delete marker boolean into an XML form.
func (b ExpireDeleteMarker) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if !b {
		return nil
	}
	type expireDeleteMarkerWrapper ExpireDeleteMarker
	return e.EncodeElement(expireDeleteMarkerWrapper(b), startElement)
}

// IsEnabled returns true if the auto delete-marker expiration is enabled
func (b ExpireDeleteMarker) IsEnabled() bool {
	return bool(b)
}

// Expiration structure - expiration details of lifecycle configuration
type Expiration struct {
	XMLName      xml.Name           `xml:"Expiration,omitempty" json:"-"`
	Date         ExpirationDate     `xml:"Date,omitempty" json:"Date,omitempty"`
	Days         ExpirationDays     `xml:"Days,omitempty" json:"Days,omitempty"`
	DeleteMarker ExpireDeleteMarker `xml:"ExpiredObjectDeleteMarker,omitempty"`
}

// MarshalJSON customizes json encoding by removing empty day/date specification.
func (e Expiration) MarshalJSON() ([]byte, error) {
	type expiration struct {
		Date         *ExpirationDate `json:"Date,omitempty"`
		Days         *ExpirationDays `json:"Days,omitempty"`
		DeleteMarker ExpireDeleteMarker
	}

	newexp := expiration{
		DeleteMarker: e.DeleteMarker,
	}
	if !e.IsDaysNull() {
		newexp.Days = &e.Days
	}
	if !e.IsDateNull() {
		newexp.Date = &e.Date
	}
	return json.Marshal(newexp)
}

// IsDaysNull returns true if days field is null
func (e Expiration) IsDaysNull() bool {
	return e.Days == ExpirationDays(0)
}

// IsDateNull returns true if date field is null
func (e Expiration) IsDateNull() bool {
	return e.Date.Time.IsZero()
}

// IsDeleteMarkerExpirationEnabled returns true if the auto-expiration of delete marker is enabled
func (e Expiration) IsDeleteMarkerExpirationEnabled() bool {
	return e.DeleteMarker.IsEnabled()
}

// IsNull returns true if both date and days fields are null
func (e Expiration) IsNull() bool {
	return e.IsDaysNull() && e.IsDateNull() && !e.IsDeleteMarkerExpirationEnabled()
}

// MarshalXML is expiration is non null
func (e Expiration) MarshalXML(en *xml.Encoder, startElement xml.StartElement) error {
	if e.IsNull() {
		return nil
	}
	type expirationWrapper Expiration
	return en.EncodeElement(expirationWrapper(e), startElement)
}

// MarshalJSON customizes json encoding by omitting empty values
func (r Rule) MarshalJSON() ([]byte, error) {
	type rule struct {
		AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `json:"AbortIncompleteMultipartUpload,omitempty"`
		Expiration                     *Expiration                     `json:"Expiration,omitempty"`
		ID                             string                          `json:"ID"`
		RuleFilter                     *Filter                         `json:"Filter,omitempty"`
		NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `json:"NoncurrentVersionExpiration,omitempty"`
		NoncurrentVersionTransition    *NoncurrentVersionTransition    `json:"NoncurrentVersionTransition,omitempty"`
		Prefix                         string                          `json:"Prefix,omitempty"`
		Status                         string                          `json:"Status"`
		Transition                     *Transition                     `json:"Transition,omitempty"`
	}
	newr := rule{
		Prefix: r.Prefix,
		Status: r.Status,
		ID:     r.ID,
	}

	if !r.RuleFilter.IsNull() {
		newr.RuleFilter = &r.RuleFilter
	}
	if !r.AbortIncompleteMultipartUpload.IsDaysNull() {
		newr.AbortIncompleteMultipartUpload = &r.AbortIncompleteMultipartUpload
	}
	if !r.Expiration.IsNull() {
		newr.Expiration = &r.Expiration
	}
	if !r.Transition.IsNull() {
		newr.Transition = &r.Transition
	}
	if !r.NoncurrentVersionExpiration.isNull() {
		newr.NoncurrentVersionExpiration = &r.NoncurrentVersionExpiration
	}
	if !r.NoncurrentVersionTransition.isNull() {
		newr.NoncurrentVersionTransition = &r.NoncurrentVersionTransition
	}

	return json.Marshal(newr)
}

// Rule represents a single rule in lifecycle configuration
type Rule struct {
	XMLName                        xml.Name                       `xml:"Rule,omitempty" json:"-"`
	AbortIncompleteMultipartUpload AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty" json:"AbortIncompleteMultipartUpload,omitempty"`
	Expiration                     Expiration                     `xml:"Expiration,omitempty" json:"Expiration,omitempty"`
	ID                             string                         `xml:"ID" json:"ID"`
	RuleFilter                     Filter                         `xml:"Filter,omitempty" json:"Filter,omitempty"`
	NoncurrentVersionExpiration    NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"  json:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransition    NoncurrentVersionTransition    `xml:"NoncurrentVersionTransition,omitempty" json:"NoncurrentVersionTransition,omitempty"`
	Prefix                         string                         `xml:"Prefix,omitempty" json:"Prefix,omitempty"`
	Status                         string                         `xml:"Status" json:"Status"`
	Transition                     Transition                     `xml:"Transition,omitempty" json:"Transition,omitempty"`
}

// Configuration is a collection of Rule objects.
type Configuration struct {
	XMLName xml.Name `xml:"LifecycleConfiguration,omitempty" json:"-"`
	Rules   []Rule   `xml:"Rule"`
}

// Empty check if lifecycle configuration is empty
func (c *Configuration) Empty() bool {
	if c == nil {
		return true
	}
	return len(c.Rules) == 0
}

// NewConfiguration initializes a fresh lifecycle configuration
// for manipulation, such as setting and removing lifecycle rules
// and filters.
func NewConfiguration() *Configuration {
	return &Configuration{}
}

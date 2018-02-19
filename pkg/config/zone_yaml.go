package config

import (
	fmt "fmt"
	"strconv"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

const numReplicasSeparator = ":"

var _ yaml.Marshaler = Constraints{}
var _ yaml.Unmarshaler = &Constraints{}

// MarshalYAML implements yaml.Marshaler.
func (c Constraints) MarshalYAML() (interface{}, error) {
	short := make([]string, len(c.Constraints))
	for i, c := range c.Constraints {
		short[i] = c.String()
	}

	// Print in the new format (i.e. "c1,c2,c3:numReplicas") if NumReplicas is
	// non-zero, otherwise something match the legacy format ([c1, c2, c3]) to
	// avoid confusing users. Note that the caller of this function has to be
	// careful to avoid creating double brackets, which would break compatability.
	if c.NumReplicas != 0 {
		return fmt.Sprintf("%s%s%d", strings.Join(short, ","), numReplicasSeparator, c.NumReplicas), nil
	}
	return short, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *Constraints) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	splitStr := strings.Split(str, numReplicasSeparator)
	if len(splitStr) != 2 {
		return fmt.Errorf("no more than one separator '%s' is allowed; got %q", numReplicasSeparator, str)
	}

	numReplicas, err := strconv.Atoi(splitStr[1])
	if err != nil {
		return fmt.Errorf("number of replicas for constraints %q must be an integer: %v", str, err)
	}

	shortConstraints := strings.Split(splitStr[0], ",")
	constraints := make([]Constraint, len(shortConstraints))
	for i, short := range shortConstraints {
		if err := constraints[i].FromString(short); err != nil {
			return err
		}
	}

	c.NumReplicas = int32(numReplicas)
	c.Constraints = constraints
	return nil
}

type constraintsList []Constraints

var _ yaml.Marshaler = constraintsList{}
var _ yaml.Unmarshaler = &constraintsList{}

// MarshalYAML implements yaml.Marshaler.
func (c constraintsList) MarshalYAML() (interface{}, error) {
	if len(c) != 1 || c[0].NumReplicas != 0 {
		return []Constraints(c), nil
	}
	return c[0].MarshalYAML()
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *constraintsList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var strs []string
	if err := unmarshal(&strs); err != nil {
		return err
	}

	if len(strs) == 0 {
		return nil
	}

	// Check how many of the strings include a NumReplicas suffix.
	var includeNumReplicas int
	for _, str := range strs {
		splitStr := strings.Split(str, ":")
		switch len(splitStr) {
		case 1:
		case 2:
			includeNumReplicas++
		default:
			return fmt.Errorf("no more than one separator ':' is allowed; got %q", str)
		}
	}
	switch includeNumReplicas {
	case 0:
		constraints := make([]Constraint, len(strs))
		for i, short := range strs {
			if err := constraints[i].FromString(short); err != nil {
				return err
			}
		}
		*c = []Constraints{
			{
				Constraints: constraints,
				NumReplicas: 0,
			},
		}
	case len(strs):
		for _, str := range strs {
			var constraints Constraints
			if err := yaml.Unmarshal([]byte(str), &constraints); err != nil {
				return err
			}
			*c = append(*c, constraints)
		}
	default:
		return fmt.Errorf(
			"either all constraints must specify a number of replicas, or none of them; got %d/%d: %v",
			includeNumReplicas, len(strs), strs)
	}
	return nil
}

// marshalableZoneConfig should be kept up-to-date with the real,
// auto-generated ZoneConfig type, but with []Constraints changed to
// constraintsList for backwards-compatible yaml marshaling and unmarshaling.
type marshalableZoneConfig struct {
	RangeMinBytes int64           `protobuf:"varint,2,opt,name=range_min_bytes,json=rangeMinBytes" json:"range_min_bytes" yaml:"range_min_bytes"`
	RangeMaxBytes int64           `protobuf:"varint,3,opt,name=range_max_bytes,json=rangeMaxBytes" json:"range_max_bytes" yaml:"range_max_bytes"`
	GC            GCPolicy        `protobuf:"bytes,4,opt,name=gc" json:"gc"`
	NumReplicas   int32           `protobuf:"varint,5,opt,name=num_replicas,json=numReplicas" json:"num_replicas" yaml:"num_replicas"`
	Constraints   constraintsList `protobuf:"bytes,6,rep,name=constraints" json:"constraints" yaml:"constraints,flow"`
	Subzones      []Subzone       `protobuf:"bytes,8,rep,name=subzones" json:"subzones" yaml:"-"`
	SubzoneSpans  []SubzoneSpan   `protobuf:"bytes,7,rep,name=subzone_spans,json=subzoneSpans" json:"subzone_spans" yaml:"-"`
}

var _ yaml.Marshaler = ZoneConfig{}
var _ yaml.Unmarshaler = &ZoneConfig{}

// MarshalYAML implements yaml.Marshaler.
func (c ZoneConfig) MarshalYAML() (interface{}, error) {
	var aux marshalableZoneConfig
	aux.RangeMinBytes = c.RangeMinBytes
	aux.RangeMaxBytes = c.RangeMaxBytes
	aux.GC = c.GC
	aux.NumReplicas = c.NumReplicas
	aux.Constraints = constraintsList(c.Constraints)
	aux.Subzones = c.Subzones
	aux.SubzoneSpans = c.SubzoneSpans
	return aux, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *ZoneConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux marshalableZoneConfig
	if err := unmarshal(&aux); err != nil {
		return err
	}
	c.RangeMinBytes = aux.RangeMinBytes
	c.RangeMaxBytes = aux.RangeMaxBytes
	c.GC = aux.GC
	c.NumReplicas = aux.NumReplicas
	c.Constraints = []Constraints(aux.Constraints)
	c.Subzones = aux.Subzones
	c.SubzoneSpans = aux.SubzoneSpans
	return nil
}

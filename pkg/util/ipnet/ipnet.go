// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ipnet

import (
	"bytes"
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// IPNet is a thin wrapper around "net".IPNet
// Go's "net" IP parsing doesn't work well for handling postgres style IP types
// 	- It discards information when parsing IPv4, forcing it to be IPv6, and then
// 		assuming IPv4-mapped IPv6 addresses are purely IPv4 (only for printing).
// 		This is solved by having a Family field
// 	- Uses extraneous bytes to store IPv4. This is solved by using a family byte
// 	  during marshalling. This uses the Family field
// 	- ParseIP and ParseCIDR are very strict, whereas postgres' INET and CIDR
// 	  have very relaxed constraints for parsing an IP.
type IPNet struct {
	// net.IPNet includes the IP and mask data
	net.IPNet
	// Family denotes what type of IP the original IP was
	Family int
}

var (
	// IPv4Family is the pgwire constant for IPv4. It is defined as AF_INET
	IPv4Family = 4
	// IPv6Family is the pgwire constant for IPv4. It is defined as IPv4Family + 1
	IPv6Family = 6
)

// IPv4Size is 1 byte for family, 1 byte for mask, 4 for IP
// FIXME(joey): should we assert net.{IPv4len,IPv6len} are the same as expected?
var IPv4Size = net.IPv4len + 2

// IPv6Size 1 byte for family, 1 byte for mask, 16 for IP
var IPv6Size = net.IPv6len + 2

// MarshalBinary returns the IPNet as a byte slice
func (u *IPNet) MarshalBinary() ([]byte, error) {
	var buffer bytes.Buffer
	// Record the family as the first byte
	err := buffer.WriteByte(byte(u.Family))

	if err != nil {
		return nil, err
	}

	// Record the mask size as the second byte
	ones, size := u.IPNet.Mask.Size()
	if ones == 0 && size == 0 {
		return nil, errors.New("IPNet Mask wasn't CIDR form")
	}

	err = buffer.WriteByte(byte(ones))
	if err != nil {
		return nil, err
	}

	// Record the IPv4 or IPv6 value (4 or 16 bytes)
	if u.Family == IPv4Family {
		// Ensure IPv4 byte slice is only len(4)
		_, err = buffer.Write(u.IPNet.IP.To4())
	} else {
		_, err = buffer.Write(u.IPNet.IP)
	}

	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// UnmarshalBinary returns the IPNet as a byte slice
func (u *IPNet) UnmarshalBinary(data []byte) error {
	u.Family = int(data[0])
	if u.Family == IPv4Family {
		u.IPNet.Mask = net.CIDRMask(int(data[1]), 32)
		u.IPNet.IP = make(net.IP, net.IPv4len)
		copy(u.IPNet.IP, data[2:IPv4Size])
	} else if u.Family == IPv6Family {
		u.IPNet.Mask = net.CIDRMask(int(data[1]), 128)
		u.IPNet.IP = make(net.IP, net.IPv6len)
		copy(u.IPNet.IP, data[2:IPv6Size])
	} else {
		// FIXME(joey): maybe throw error or some kind of decoding or IPNet error
		panic("Got bad family in binary")
	}

	return nil
}

// String will convert the IP to the appropriate family formatted string
// representation. In order to retain postgres compatibility we ensure
// IPv4-mapped IPv6 stays in IPv6 format, unlike net.IP.String()
func (u *IPNet) String() string {
	isIPv4MappedIPv6 := u.Family == IPv6Family && u.IPNet.IP.Equal(u.IPNet.IP.To4())
	ones, size := u.IPNet.Mask.Size()
	if ones == size && isIPv4MappedIPv6 {
		return "::ffff:" + u.IPNet.IP.String()
	} else if ones == size {
		return u.IPNet.IP.String()
	} else if isIPv4MappedIPv6 {
		// Due to an issue with IPv4-mapped IPv6 the mask is also reduced to an IPv4
		// mask during net.IPNet.String, so we need to add the mask manually
		return "::ffff:" + u.IP.String() + "/" + strconv.Itoa(ones)
	}
	return u.IPNet.String()
}

// Compare two IPNets. IPv4-mapped IPv6 addresses are not equal to their IPv4
// mapping. The order of order importance goes Family > Mask > IP-bytes
func (u IPNet) Compare(other *IPNet) int {
	if u.Family < other.Family {
		return -1
	} else if u.Family > other.Family {
		return 1
	}

	// We assume that Masks are normalized in []byte length automatically. This is
	// an assumption with how we parse and unmarshal, in particular because we use
	// net.CIDRMask. IPs do not have normalized []byte length, so we need to make
	// sure to normalize both IPs if comparing IPv4
	if cmp := bytes.Compare(u.IPNet.Mask, other.IPNet.Mask); cmp != 0 {
		return cmp
	} else if u.Family == IPv4Family {
		return bytes.Compare(u.IPNet.IP.To4(), other.IPNet.IP.To4())
	}
	return bytes.Compare(u.IPNet.IP, other.IPNet.IP)
}

// Equal checks if the family, mask, and IP are equal. net.IP.Equal will be true
// if the families are different, e.g. ::ffff:192.168.0.1 == 192.168.0.1
func (u *IPNet) Equal(other *IPNet) bool {
	return u.Family == other.Family && u.IPNet.IP.Equal(other.IPNet.IP) && bytes.Equal(u.IPNet.Mask, other.IPNet.Mask)
}

// Of the form:
// 	"192.168" => "192.168.0.0/16"
// 	"192.168/24" => "192.168.0.0/24"
// 	"192.168.1.2" => "192.168.1.2/32"
// 	"192.168.1.2/16" => errors.New("Value has bits set to right of mask.")
// func ParseCIDR(s string) (IPNet, error) {
// 	// TODO(joey):
// 	return IPNet{}, nil
// }

// getFamily checks what family the ip is in. If it doesn't appear to match
// either, getFamily returns -1
func getFamily(addr string) int {
	// Get the family of the IP.
	for i := 0; i < len(addr); i++ {
		switch addr[i] {
		case '.':
			return IPv4Family
		case ':':
			return IPv6Family
		}
	}
	// Default to IPv4, as we need to handle '192/10'
	return IPv4Family
}

// ParseINet parses postgres style INET types. See TestIPNetParseINet for
// examples.
func ParseINet(s string) (*IPNet, error) {
	i := strings.IndexByte(s, '/')
	family := getFamily(s)
	var mask net.IPMask

	// If no mask suffix was provided, implicitly don't mask
	if i < 0 {
		// Trims IPv4 suffix "." to match postgres compitibility
		addr := s
		if family == IPv4Family {
			addr = strings.TrimRight(addr, ".")
		}
		ip := net.ParseIP(addr)
		if ip == nil {
			return nil, errors.Errorf("could not parse %q as inet. invalid IP", s)
		}

		if family == IPv4Family {
			mask = net.CIDRMask(32, 32)
			// Ensures that the IP is len(4)
			ip = ip.To4()
		} else {
			mask = net.CIDRMask(128, 128)
		}

		return &IPNet{Family: family, IPNet: net.IPNet{
			IP:   ip,
			Mask: mask,
		}}, nil
	}

	addr, maskStr := s[:i], s[i+1:]
	// Trims IPv4 suffix "." to match postgres compitibility
	if family == IPv4Family {
		addr = strings.TrimRight(addr, ".")
	}
	maskOnes, err := strconv.Atoi(maskStr)
	if err != nil {
		return nil, errors.Errorf("could not parse %q as inet. invalid mask", s)
	} else if maskOnes < 0 || (family == IPv4Family && maskOnes > 32) || (family == IPv6Family && maskOnes > 128) {
		return nil, errors.Errorf("could not parse %q as inet. invalid mask", s)
	}

	if family == IPv4Family {
		// If the mask is outside the defined octets, postgres will raise an error
		octetCount := strings.Count(addr, ".") + 1
		if (octetCount+1)*8-1 < maskOnes {
			return nil, errors.Errorf("could not parse %q as inet. mask is larger than provided octets", s)
		}

		// Append extra ".0" to ensure there are a total of 4 octets
		var buffer bytes.Buffer
		buffer.WriteString(addr)
		for i := 0; i < 4-octetCount; i++ {
			buffer.WriteString(".0")
		}
		addr = buffer.String()

	}

	ip := net.ParseIP(addr)
	if ip == nil {
		return nil, errors.Errorf("could not parse %q as inet. invalid IP", s)
	}

	if family == IPv4Family {
		mask = net.CIDRMask(maskOnes, 32)
	} else {
		mask = net.CIDRMask(maskOnes, 128)
	}

	return &IPNet{Family: family, IPNet: net.IPNet{
		IP:   ip,
		Mask: mask,
	}}, nil
}

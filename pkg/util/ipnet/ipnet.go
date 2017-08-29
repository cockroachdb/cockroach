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
	"math/rand"
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// IPNet is a thin wrapper around "net".IPNet.
// Go's "net" IP parsing doesn't work well for handling postgres style IP types.
// 	- It discards information when parsing IPv4, forcing it to be IPv6, and then
// 		assuming IPv4-mapped IPv6 addresses are purely IPv4 (only for printing).
// 		This is solved by having a Family field.
// 	- Uses extraneous bytes to store IPv4. This is solved by using a family byte
// 	  during marshalling. This uses the Family field.
// 	- ParseIP and ParseCIDR are very strict, whereas postgres' INET and CIDR
// 	  have very relaxed constraints for parsing an IP.
type IPNet struct {
	// net.IPNet includes the IP and mask data.
	net.IPNet
	// Family denotes what type of IP the original IP was.
	Family IPFamily
}

// IPFamily denotes which classification the IP belongs to.
type IPFamily int

const (
	// IPv4Family is for IPs in the IPv4 space.
	IPv4Family IPFamily = iota
	// IPv6Family is for IPs in the IPv6 space.
	IPv6Family
)

// IPv4Size is 1 byte for family, 1 byte for mask, 4 for IP.
// FIXME(joey): should we assert net.{IPv4len,IPv6len} are the same as expected?
var IPv4Size = net.IPv4len + 2

// IPv6Size 1 byte for family, 1 byte for mask, 16 for IP.
var IPv6Size = net.IPv6len + 2

// ToBuffer appends the IPNet encoding to a buffer and returns the final buffer.
func (u *IPNet) ToBuffer(appendTo []byte) []byte {
	ones, _ := u.Mask.Size()
	// Record the family as the first byte and the mask size as the second byte.
	appendTo = append(appendTo, byte(u.Family), byte(ones))
	// Record the IPv4 or IPv6 value (4 or 16 bytes).
	if u.Family == IPv4Family {
		// Ensure IPv4 byte slice is only len(4).
		appendTo = append(appendTo, u.IP.To4()...)
	} else {
		appendTo = append(appendTo, u.IP...)
	}
	return appendTo
}

// populateMask will populate the IPMask byte slice with the amount of ones and
// remaining zeros. This is pulled directly from net.CIDRMask in order to do
// batch allocation on the []byte that CIDRMask makes otherwise. See
// https://golang.org/src/net/ip.go#L78 for the beginning of the copied code.
func populateMask(mask net.IPMask, ones uint, family IPFamily) net.IPMask {
	var maskSize int
	if family == IPv4Family {
		maskSize = 32
	} else {
		maskSize = 128
	}
	l := maskSize / 8
	for i := 0; i < l; i++ {
		if ones >= 8 {
			mask[i] = 0xff
			ones -= 8
			continue
		}
		mask[i] = ^byte(0xff >> ones)
		ones = 0
	}
	return mask
}

// FromBuffer populates an IPNet with data from a byte slice, returning the
// remaining buffer or an error.
func (u *IPNet) FromBuffer(data []byte) ([]byte, error) {
	u.Family = IPFamily(data[0])
	if u.Family == IPv4Family {
		buf := make([]byte, net.IPv4len*2)
		u.IP = net.IP(buf[:net.IPv4len])
		u.Mask = net.IPMask(buf[net.IPv4len:])
		maskOnes := uint(data[1])
		u.Mask = populateMask(u.Mask, maskOnes, u.Family)
		copy(u.IP, data[2:IPv4Size])
		return data[IPv4Size:], nil
	} else if u.Family == IPv6Family {
		buf := make([]byte, net.IPv6len*2)
		u.IP = net.IP(buf[:net.IPv6len])
		u.Mask = net.IPMask(buf[net.IPv6len:])
		maskOnes := uint(data[1])
		u.Mask = populateMask(u.Mask, maskOnes, u.Family)
		copy(u.IP, data[2:IPv6Size])
		return data[IPv6Size:], nil
	}
	return nil, errors.Errorf("IPNet decoding error: bad family, got %d", u.Family)
}

// String will convert the IP to the appropriate family formatted string
// representation. In order to retain postgres compatibility we ensure
// IPv4-mapped IPv6 stays in IPv6 format, unlike net.IP.String().
func (u IPNet) String() string {
	isIPv4MappedIPv6 := u.Family == IPv6Family && u.IP.Equal(u.IP.To4())
	ones, size := u.Mask.Size()
	if ones == size && isIPv4MappedIPv6 {
		return "::ffff:" + u.IP.String()
	} else if ones == size {
		return u.IP.String()
	} else if isIPv4MappedIPv6 {
		// Due to an issue with IPv4-mapped IPv6 the mask is also reduced to an IPv4
		// mask during net.IPNet.String, so we need to add the mask manually.
		return "::ffff:" + u.IP.String() + "/" + strconv.Itoa(ones)
	}
	return u.IPNet.String()
}

// Compare two IPNets. IPv4-mapped IPv6 addresses are not equal to their IPv4
// mapping. The order of order importance goes Family > Mask > IP-bytes.
func (u IPNet) Compare(other *IPNet) int {
	if u.Family < other.Family {
		return -1
	} else if u.Family > other.Family {
		return 1
	}

	// We assume that Masks are normalized in []byte length automatically. This is
	// an assumption with how we parse and unmarshal, in particular because we use
	// net.CIDRMask. IPs do not have normalized []byte length, so we need to make
	// sure to normalize both IPs if comparing IPv4.
	if cmp := bytes.Compare(u.Mask, other.Mask); cmp != 0 {
		return cmp
	} else if u.Family == IPv4Family {
		return bytes.Compare(u.IP.To4(), other.IP.To4())
	}
	return bytes.Compare(u.IP, other.IP)
}

// Equal checks if the family, mask, and IP are equal. net.IP.Equal will be true
// if the families are different, e.g. ::ffff:192.168.0.1 == 192.168.0.1.
func (u *IPNet) Equal(other *IPNet) bool {
	return u.Family == other.Family && u.IP.Equal(other.IP) && bytes.Equal(u.Mask, other.Mask)
}

// getFamily checks what family the ip is in. If it doesn't appear to match
// either, getFamily returns -1.
func getFamily(addr string) IPFamily {
	// Get the family of the IP.
	for i := 0; i < len(addr); i++ {
		switch addr[i] {
		case '.':
			return IPv4Family
		case ':':
			return IPv6Family
		}
	}
	// Default to IPv4, as we need to handle '192/10'.
	return IPv4Family
}

// ParseINet parses postgres style INET types. See TestIPNetParseINet for
// examples.
func ParseINet(s string, dest *IPNet) error {
	i := strings.IndexByte(s, '/')
	family := getFamily(s)
	var mask net.IPMask

	// If no mask suffix was provided, implicitly don't mask.
	if i < 0 {
		// Trims IPv4 suffix "." to match postgres compitibility.
		addr := s
		if family == IPv4Family {
			addr = strings.TrimRight(addr, ".")
		}
		ip := net.ParseIP(addr)
		if ip == nil {
			return errors.Errorf("could not parse %q as inet. invalid IP", s)
		}

		if family == IPv4Family {
			mask = net.CIDRMask(32, 32)
			// Ensures that the IP is len(4).
			ip = ip.To4()
		} else {
			mask = net.CIDRMask(128, 128)
		}

		*dest = IPNet{Family: family, IPNet: net.IPNet{
			IP:   ip,
			Mask: mask,
		}}
		return nil
	}

	addr, maskStr := s[:i], s[i+1:]
	// Trims IPv4 suffix "." to match postgres compitibility.
	if family == IPv4Family {
		addr = strings.TrimRight(addr, ".")
	}
	maskOnes, err := strconv.Atoi(maskStr)
	if err != nil {
		return errors.Errorf("could not parse %q as inet. invalid mask", s)
	} else if maskOnes < 0 || (family == IPv4Family && maskOnes > 32) || (family == IPv6Family && maskOnes > 128) {
		return errors.Errorf("could not parse %q as inet. invalid mask", s)
	}

	if family == IPv4Family {
		// If the mask is outside the defined octets, postgres will raise an error.
		octetCount := strings.Count(addr, ".") + 1
		if (octetCount+1)*8-1 < maskOnes {
			return errors.Errorf("could not parse %q as inet. mask is larger than provided octets", s)
		}

		// Append extra ".0" to ensure there are a total of 4 octets.
		var buffer bytes.Buffer
		buffer.WriteString(addr)
		for i := 0; i < 4-octetCount; i++ {
			buffer.WriteString(".0")
		}
		addr = buffer.String()

	}

	ip := net.ParseIP(addr)
	if ip == nil {
		return errors.Errorf("could not parse %q as inet. invalid IP", s)
	}

	if family == IPv4Family {
		mask = net.CIDRMask(maskOnes, 32)
	} else {
		mask = net.CIDRMask(maskOnes, 128)
	}

	*dest = IPNet{Family: family, IPNet: net.IPNet{
		IP:   ip,
		Mask: mask,
	}}
	return nil
}

// RandIPNet generates a random IPNet. This includes random mask size and IP
// family.
func RandIPNet(rng *rand.Rand) IPNet {
	var ipNet IPNet
	if rng.Intn(2) > 0 {
		ipNet.Family = IPv4Family
		ipNet.IP = net.IPv4(byte(rng.Intn(256)), byte(rng.Intn(256)), byte(rng.Intn(256)), byte(rng.Intn(256)))
		ipNet.Mask = net.CIDRMask(rng.Intn(33), 32)
	} else {
		ipNet.Family = IPv6Family
		for i := 0; i < net.IPv6len; i++ {
			ipNet.IP = append(ipNet.IP, byte(rng.Intn(256)))
		}
		ipNet.Mask = net.CIDRMask(rng.Intn(129), 128)
	}
	return ipNet
}

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

package ipaddr

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/pkg/errors"
)

// Addr is the representation of the IP address. The Uint128 takes 16-bytes for
// both IPv4 and IPv6.
type Addr uint128.Uint128

// IPAddr stores an IP address's family, IP, and host mask. This was chosen over
// Go's "net" IP, as that struct doesn't work well for what we need to do.
// 	- It discards information when parsing IPv4, forcing it to be IPv6, and then
// 		assuming IPv4-mapped IPv6 addresses are purely IPv4 (only for printing).
// 		This is solved by having a Family field.
// 	- ParseIP and ParseCIDR are very strict, whereas postgres' INET and CIDR
// 	  have very relaxed constraints for parsing an IP.
//  - Doing int64 operations is much more efficient than byte slice operations.
type IPAddr struct {
	// Family denotes what type of IP the original IP was.
	Family IPFamily
	Mask   byte
	Addr   Addr
}

// IPFamily denotes which classification the IP belongs to.
type IPFamily byte

const (
	// IPv4Family is for IPs in the IPv4 space.
	IPv4Family IPFamily = iota
	// IPv6Family is for IPs in the IPv6 space.
	IPv6Family
)

// IPv6size 1 byte for family, 1 byte for mask, 16 for IP.
var IPv6size = net.IPv6len + 2

// ToBuffer appends the IPAddr encoding to a buffer and returns the final buffer.
func (ipAddr *IPAddr) ToBuffer(appendTo []byte) []byte {
	// Record the family as the first byte and the mask size as the second byte.
	appendTo = append(appendTo, byte(ipAddr.Family), ipAddr.Mask)
	// TODO(joey): We could save on bytes via. reducing IPv4 size. We will also
	// need to change the PeekValueLength for DIPNet.
	appendTo = uint128.Uint128(ipAddr.Addr).AppendBytes(appendTo)
	return appendTo
}

// FromBuffer populates an IPAddr with data from a byte slice, returning the
// remaining buffer or an error.
func (ipAddr *IPAddr) FromBuffer(data []byte) ([]byte, error) {
	ipAddr.Family = IPFamily(data[0])
	if ipAddr.Family != IPv4Family && ipAddr.Family != IPv6Family {
		return nil, errors.Errorf("IPAddr decoding error: bad family, got %d", ipAddr.Family)
	}
	ipAddr.Mask = data[1]
	ipAddr.Addr = Addr(uint128.FromBytes(data[2:]))
	return data[2+uint128.Uint128len:], nil
}

// String will convert the IP to the appropriate family formatted string
// representation. In order to retain postgres compatibility we ensure
// IPv4-mapped IPv6 stays in IPv6 format, unlike net.Addr.String().
func (ipAddr IPAddr) String() string {
	ip := net.IP(uint128.Uint128(ipAddr.Addr).GetBytes())
	isIPv4MappedIPv6 := ipAddr.Family == IPv6Family && ip.Equal(ip.To4())
	var maskSize byte

	if ipAddr.Family == IPv4Family {
		maskSize = 32
	} else {
		maskSize = 128
	}
	if ipAddr.Mask == maskSize && isIPv4MappedIPv6 {
		return "::ffff:" + ip.String()
	} else if ipAddr.Mask == maskSize {
		return ip.String()
	} else if isIPv4MappedIPv6 {
		// Due to an issue with IPv4-mapped IPv6 the mask is also reduced to an IPv4
		// mask during net.IPNet.String, so we need to add the mask manually.
		return "::ffff:" + ip.String() + "/" + strconv.Itoa(int(ipAddr.Mask))
	}
	return ip.String() + "/" + strconv.Itoa(int(ipAddr.Mask))
}

// Compare two IPAddrs. IPv4-mapped IPv6 addresses are not equal to their IPv4
// mapping. The order of order importance goes Family > Mask > IP-bytes.
func (ipAddr IPAddr) Compare(other *IPAddr) int {
	if ipAddr.Family < other.Family {
		return -1
	} else if ipAddr.Family > other.Family {
		return 1
	}

	if ipAddr.Mask < other.Mask {
		return -1
	} else if ipAddr.Mask > other.Mask {
		return 1
	}
	return ipAddr.Addr.Compare(other.Addr)
}

// Equal checks if the family, mask, and IP are equal.
func (ipAddr *IPAddr) Equal(other *IPAddr) bool {
	return ipAddr.Family == other.Family && ipAddr.Mask == other.Mask && ipAddr.Addr.Equal(other.Addr)
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

// ParseINet parses postgres style INET types. See TestIPAddrParseINet for
// examples.
func ParseINet(s string, dest *IPAddr) error {
	var maskSize byte
	i := strings.IndexByte(s, '/')
	family := getFamily(s)

	// If no mask suffix was provided, implicitly don't mask.
	if i < 0 {
		// Trims IPv4 suffix "." to match postgres compitibility.
		addr := s
		if family == IPv4Family {
			addr = strings.TrimRight(addr, ".")
			maskSize = 32
		} else {
			maskSize = 128
		}
		ip := net.ParseIP(addr)
		if ip == nil {
			return errors.Errorf("could not parse %q as inet. invalid IP", s)
		}

		*dest = IPAddr{Family: family,
			Addr: Addr(uint128.FromBytes(ip)),
			Mask: maskSize,
		}
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

	*dest = IPAddr{Family: family,
		Addr: Addr(uint128.FromBytes(ip)),
		Mask: byte(maskOnes),
	}
	return nil
}

// RandIPAddr generates a random IPAddr. This includes random mask size and IP
// family.
func RandIPAddr(rng *rand.Rand) IPAddr {
	var ipAddr IPAddr
	if rng.Intn(2) > 0 {
		ipAddr.Family = IPv4Family
		ipAddr.Mask = byte(rng.Intn(33))
		ipAddr.Addr = Addr(uint128.FromInts(0, uint64(rng.Uint32())))
	} else {
		ipAddr.Family = IPv6Family
		ipAddr.Mask = byte(rng.Intn(129))
		ipAddr.Addr = Addr(uint128.FromInts(rng.Uint64(), rng.Uint64()))
	}
	return ipAddr
}

// WriteIPv4Bytes writes the 4-byte IPv4 representation. If the IP is IPv6 then
// the first 12-bytes are truncated.
func (ip Addr) WriteIPv4Bytes(writer io.Writer) error {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], ip.Lo)
	_, err := writer.Write(tmp[4:])
	return err
}

// WriteIPv6Bytes gets the 16-byte IPv6 representation.
func (ip Addr) WriteIPv6Bytes(writer io.Writer) error {
	var tmp [16]byte
	binary.BigEndian.PutUint64(tmp[:8], ip.Hi)
	binary.BigEndian.PutUint64(tmp[8:], ip.Lo)
	_, err := writer.Write(tmp[:])
	return err
}

// Equal wraps the Uint128 equivilance.
func (ip Addr) Equal(o Addr) bool {
	return uint128.Uint128(ip).Equal(uint128.Uint128(o))
}

// Compare wraps the Uint128 equivilance.
func (ip Addr) Compare(o Addr) int {
	return uint128.Uint128(ip).Compare(uint128.Uint128(o))
}

// Sub wraps the Uint128 subtraction.
func (ip Addr) Sub(o uint64) Addr {
	return Addr(uint128.Uint128(ip).Sub(o))
}

// Add wraps the Uint128 addition.
func (ip Addr) Add(o uint64) Addr {
	return Addr(uint128.Uint128(ip).Add(o))
}

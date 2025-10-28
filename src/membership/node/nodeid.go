// Package nodeid defines the immutable identity for a node (IPv4 endpoint + boot
// timestamp). The boot timestamp ("boot version") distinguishes successive
// versions of the *same machine* (e.g., after a crash and rejoin). The
// SWIM-style *incarnation number* is intentionally **not** part of NodeID and
// should be tracked separately by the membership store for status overrides.
package nodeid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

// Endpoint is an IPv4 UDP address (IP + Port).
type Endpoint struct {
	IP   netip.Addr // IPv4 only
	Port uint16     // UDP port
}

// ParseStringToEndpoint parses "a.b.c.d:port" into an Endpoint.
// Converts IP information from string to Endpoint struct.
func ParseStringToEndpoint(s string) (Endpoint, error) {
	ap, err := netip.ParseAddrPort(s)
	if err != nil {
		return Endpoint{}, err
	}
	if !ap.Addr().Is4() {
		return Endpoint{}, errors.New("nodeid: only IPv4 endpoints are supported")
	}
	return Endpoint{IP: ap.Addr(), Port: ap.Port()}, nil
}

// String returns canonical dotted form: "a.b.c.d:port".
// Parses Endpoint struct to return string representation.
func (e Endpoint) EndpointToString() string {
	return fmt.Sprintf("%s:%d", e.IP.String(), e.Port)
}

// IsZero reports whether the endpoint is empty.
func (e Endpoint) IsZero() bool { return !e.IP.IsValid() }

// NodeID identifies a specific *booted version* of a node at an Endpoint.
// It does NOT include the SWIM incarnation; that belongs to membership state.
type NodeID struct {
	EP        Endpoint
	TimeStamp uint64 // wall-clock at boot (nanoseconds since Unix epoch)
}

// ErrParse is returned when parsing a textual NodeID fails.
var ErrParse = errors.New("nodeid: parse error")

// New returns a NodeID for the given endpoint with the current time as boot version.
func New(ep Endpoint) NodeID { return NodeID{EP: ep, TimeStamp: bootTimeNow()} }

// String renders "a.b.c.d:port@<unixTimeStamp>" (no incarnation).
func (n NodeID) NodeIDToString() string {
	return fmt.Sprintf("%s@%d", n.EP.EndpointToString(), n.TimeStamp)
}

// GetTimestamp returns the boot timestamp of the NodeID.
func (n NodeID) GetTimestamp() uint64 {
	return n.TimeStamp
}

// Parse parses a NodeID string produced by String(): "a.b.c.d:port@<unixTimeStamp>".
func ParseStringToNodeID(s string) (NodeID, error) {
	parts := strings.Split(s, "@")
	if len(parts) != 2 {
		return NodeID{}, ErrParse
	}
	ep, err := ParseStringToEndpoint(parts[0])
	if err != nil {
		return NodeID{}, err
	}
	boot, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return NodeID{}, ErrParse
	}
	return NodeID{EP: ep, TimeStamp: boot}, nil
}

// MustParse panics on parse failure (useful in tests).
func MustParse(s string) NodeID {
	n, err := ParseStringToNodeID(s)
	if err != nil {
		panic(err)
	}
	return n
}

// TODO: remove all unused methods below
// MarshalBinary encodes NodeID in a compact, network byte order (big-endian) binary format:
//
//	4 bytes  IPv4
//	2 bytes  Port (big-endian)
//	8 bytes  TimeStamp (big-endian)
func (n NodeID) MarshalBinary() ([]byte, error) {
	if n.EP.IsZero() {
		return nil, errors.New("nodeid: zero endpoint")
	}
	ip4 := n.EP.IP.As4() // Returns [4]byte
	buf := make([]byte, 4+2+8)
	copy(buf[0:4], ip4[:])                          // Didn't use binary.BigEndian since ipv4 is already a byte array
	binary.BigEndian.PutUint16(buf[4:6], n.EP.Port) // Need to convert integers to byte array
	binary.BigEndian.PutUint64(buf[6:14], n.TimeStamp)
	return buf, nil
}

// UnmarshalBinary decodes NodeID from MarshalBinary format.
func (n *NodeID) UnmarshalBinary(b []byte) error {
	if len(b) < 14 {
		return errors.New("nodeid: short buffer")
	}
	ip, ok := netip.AddrFromSlice(b[0:4])
	if !ok || !ip.Is4() {
		return errors.New("nodeid: invalid IPv4 bytes")
	}
	port := binary.BigEndian.Uint16(b[4:6])
	boot := binary.BigEndian.Uint64(b[6:14])
	*n = NodeID{EP: Endpoint{IP: ip, Port: port}, TimeStamp: boot}
	return nil
}

// MarshalText implements encoding.TextMarshaler using String().
func (n NodeID) MarshalText() ([]byte, error) { return []byte(n.NodeIDToString()), nil }

// UnmarshalText implements encoding.TextUnmarshaler using Parse().
func (n *NodeID) UnmarshalText(b []byte) error {
	id, err := ParseStringToNodeID(string(b))
	if err != nil {
		return err
	}
	*n = id
	return nil
}

// Validate performs basic sanity checks.
func (n NodeID) Validate() error {
	if n.EP.IsZero() {
		return errors.New("nodeid: zero endpoint")
	}
	if n.EP.Port == 0 {
		return errors.New("nodeid: port must be > 0")
	}
	if !n.EP.IP.Is4() {
		return errors.New("nodeid: only IPv4 allowed")
	}
	return nil
}

// bootTimeNow yields the current Unix time in nanoseconds for constructing a boot version.
func bootTimeNow() uint64 { return uint64(time.Now().UnixNano()) }

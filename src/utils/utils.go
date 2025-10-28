package generic_utils

import (
	"net"
	"strings"
)

func ResolveDNSFromIP(nodeid string) string {
	ip := strings.Split(nodeid, ":")[0]
	names, err := net.LookupAddr(ip)
	if err != nil {
		return "unknown-host"
	}
	return names[0]
}

package statsd

import "strings"

// HostKey normalize host strings by replace dot and colon to underline
func HostKey(h string) string {
	return strings.Replace(strings.Replace(h, ".", "_", -1), ":", "_", -1)
}

// daemon/config_diff.go
package daemon

import (
	"fmt"
	"log"
	"strings"
	"text/tabwriter"
	"time"

	"hydfs-g33/config"
)

const (
	ansiReset  = "\x1b[0m"
	ansiYellow = "\x1b[33m"
)

func logConfigDiff(prev, next config.Config) {
	var b strings.Builder
	fmt.Fprintf(&b, "Config change (version %d → %d) @ %s\n",
		prev.Version, next.Version, time.Now().Format("2006-01-02 15:04:05"))

	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Field\tPrev\tNext\tΔ")
	fmt.Fprintln(w, "-----\t----\t----\t-")

	add := func(name, pv, nv string, changed bool) {
		mark := ""
		if changed {
			// color only the new value; keep logs readable if piped to files
			nv = ansiYellow + nv + ansiReset
			mark = "✱"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", name, pv, nv, mark)
	}

	// Cluster-wide knobs (the ones that change at runtime)
	add("Mode", string(prev.Mode), string(next.Mode), prev.Mode != next.Mode)
	add("PingEvery", prev.PingEvery.String(), next.PingEvery.String(), prev.PingEvery != next.PingEvery)
	add("PingFanout", fmt.Sprint(prev.PingFanout), fmt.Sprint(next.PingFanout), prev.PingFanout != next.PingFanout)
	add("GossipPeriod", prev.GossipPeriod.String(), next.GossipPeriod.String(), prev.GossipPeriod != next.GossipPeriod)
	add("GossipFanout", fmt.Sprint(prev.GossipFanout), fmt.Sprint(next.GossipFanout), prev.GossipFanout != next.GossipFanout)
	add("TSuspect", prev.TSuspect.String(), next.TSuspect.String(), prev.TSuspect != next.TSuspect)
	add("TFail", prev.TFail.String(), next.TFail.String(), prev.TFail != next.TFail)
	add("TCleanup", prev.TCleanup.String(), next.TCleanup.String(), prev.TCleanup != next.TCleanup)
	add("DropRateRecv", fmt.Sprintf("%.3f", prev.DropRateRecv), fmt.Sprintf("%.3f", next.DropRateRecv), prev.DropRateRecv != next.DropRateRecv)

	_ = w.Flush()
	log.Print("\n" + b.String())
}

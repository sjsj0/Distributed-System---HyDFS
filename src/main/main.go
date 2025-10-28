package main

import (
	"fmt"
	"hydfs-g33/config"
	hydfs_daemon "hydfs-g33/hydfs/cmd/hydfsd"
	"hydfs-g33/membership/daemon"
	nodeid "hydfs-g33/membership/node"
	"hydfs-g33/membership/store"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// resolve hostname:port (or ip:port) to netip.AddrPort
func resolveAddrPortUDP(s string) (netip.AddrPort, error) {
	ua, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		return netip.AddrPort{}, err
	}
	if ua == nil || ua.IP == nil {
		return netip.AddrPort{}, fmt.Errorf("could not resolve %q to an IP", s)
	}
	ip, ok := netip.AddrFromSlice(ua.IP)
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("bad IP bytes for %q", s)
	}
	return netip.AddrPortFrom(ip, uint16(ua.Port)), nil
}

func generateNodeID(cfg config.Config) nodeid.NodeID {
	var ap netip.AddrPort
	var err error

	if cfg.Env == "dev" {
		ap, err = netip.ParseAddrPort(strings.TrimSpace(cfg.SelfAddr))
		if err != nil {
			log.Fatal(fmt.Errorf("invalid self addr: %w", err))
		}
	} else {
		ap, err = resolveAddrPortUDP(strings.TrimSpace(cfg.SelfAddr))
		if err != nil {
			log.Fatal(fmt.Errorf("invalid self addr: %w", err))
		}
	}

	endpoint := &nodeid.Endpoint{IP: ap.Addr(), Port: ap.Port()}
	return nodeid.New(*endpoint)
}

func main() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	cfg, err := config.LoadFromFlags()

	if err != nil {
		log.Fatal(err)
	}

	selfNodeID := generateNodeID(cfg)
	st := store.New(selfNodeID, cfg)

	// Start HyDFS Daemon
	hydfsd, err := hydfs_daemon.Run(cfg, st, selfNodeID) //TODO: start these in diff goroutines
	if err != nil {
		log.Fatal(err)
	}
	defer hydfsd.Close()

	// Start Membership Daemon
	d, err := daemon.Run(cfg, st, selfNodeID)
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// ----------------- Start hydfsctl in a new tmux window -----------------
	// idStr := selfNodeID.NodeIDToString() // or fmt.Sprintf("%x", selfNodeID)

	// cmd := exec.Command("tmux", "new-window",
	// 	"hydfsctl", "--addr", cfg.SelfAddr, "--nodeid", idStr, //TODO: replace hydfsctl with appropriate command
	// )
	// // or: "tmux", "split-window", "-v", "hydfsctl --addr ... --nodeid ..."
	// cmd.Stdout = os.Stdout
	// cmd.Stderr = os.Stderr
	// _ = cmd.Start()
	// -----------------------------------------------------------------------

	// Graceful shutdown on SIGINT/SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

//package main
//
//import (
//"fmt"
//"net"
//)
//
//func main() {
//	ips, err := net.LookupIP("fa25-cs425-3301.cs.illinois.edu")
//	if err != nil {
//		panic(err)
//	}
//	for _, ip := range ips {
//		fmt.Println(ip.String())
//	}
//}

//DNS -> 130.126.2.131
//nslookup 172.22.95.38 130.126.2.131

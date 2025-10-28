package daemon

import (
	"encoding/json"
	"fmt"
	"hydfs-g33/config"
	nodeid "hydfs-g33/membership/node"
	"hydfs-g33/membership/protocol/common"
	"hydfs-g33/membership/protocol/gossip"
	"hydfs-g33/membership/protocol/pingack"
	"hydfs-g33/membership/store"
	"hydfs-g33/membership/suspicion"
	"hydfs-g33/membership/transport"
	colors "hydfs-g33/membership/utils"
	"hydfs-g33/membership/wire"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Daemon struct { //TODO: rename to MembershipDaemon
	st  *store.Store
	net *transport.UDP

	mode     atomic.Value // fast path for current mode
	loopPA   *pingack.Loop
	loopGo   *gossip.Loop
	sm       *suspicion.Manager
	inflight *common.Inflight // <-- shared

	cfg   config.Config
	cfgMu sync.RWMutex // central lock for live config
}

func Run(cfg config.Config, st *store.Store, selfNodeID nodeid.NodeID) (*Daemon, error) {

	udp, err := transport.Listen(cfg.BindAddr)
	if err != nil {
		return nil, err
	}

	d := &Daemon{
		st:       st,
		net:      udp,
		cfg:      cfg,
		inflight: common.NewInflight(), // <-- one instance
	}
	d.mode.Store(cfg.Mode)

	// Suspicion manager
	d.sm = &suspicion.Manager{
		St:             st,
		TSuspect:       cfg.TSuspect,
		TFail:          cfg.TFail,
		TCleanup:       cfg.TCleanup,
		Inflight:       d.inflight,                     // share
		UsePingTimeout: cfg.Mode == config.ModePingAck, // gate by mode
	}
	go d.sm.Run()

	// --------------------------------- INTRODUCER JOIN ---------------------------------
	if cfg.IsIntroducer {
		// Starting the introducer server in a separate goroutine so that it doesn't block the main thread
		go d.startIntroducerServer(cfg.IntroducerBindAddr)
	} else {
		// Blocking the main thread until we successfully join the cluster
		d.joinCluster(cfg.Introducer)
	}

	// -----------------------------------------------------------------------------------

	// Start protocol according to current config
	d.startProtocol(cfg)

	// Membership HTTP (CLI path). No “announce now”—loops piggyback on their next tick.
	go d.membershipHTTP(cfg.MembershipHTTP)

	return d, nil
}

func (d *Daemon) startProtocol(cfg config.Config) {
	switch cfg.Mode {
	case config.ModePingAck:
		d.stopGossip()
		d.loopPA = pingack.New(d.st, d.net, cfg.PingEvery, cfg.TSuspect, cfg.PingFanout)
		d.loopPA.DropRateRecv = cfg.DropRateRecv
		// Wire config piggyback and inbound apply
		d.loopPA.SetConfigDTO(cfg.ToDTO())
		d.loopPA.OnConfig = d.onConfigFromWire
		d.loopPA.Run(64 << 10)

	case config.ModeGossip:
		d.stopPingAck()
		d.loopGo = gossip.New(d.st, d.net, cfg.GossipPeriod, cfg.GossipFanout)
		d.loopGo.DropRateRecv = cfg.DropRateRecv
		// Wire config piggyback and inbound apply
		d.loopGo.SetConfigDTO(cfg.ToDTO())
		d.loopGo.OnConfig = d.onConfigFromWire
		d.loopGo.Run(64 << 10)
	}
	d.mode.Store(cfg.Mode)
}

func (d *Daemon) stopPingAck() {
	if d.loopPA != nil {
		d.loopPA.Close()
		d.loopPA = nil
	}
}

func (d *Daemon) stopGossip() {
	if d.loopGo != nil {
		d.loopGo.Close()
		d.loopGo = nil
	}
}

// applyRuntime pushes updated knobs into running components and restarts loops if the mode changed.
// It also refreshes each loop's cached ConfigDTO so the next normal send carries the latest config.
func (d *Daemon) applyRuntime(prev, next config.Config) {

	// Log the diff first
	logConfigDiff(prev, next)

	modeChanged := prev.Mode != next.Mode

	switch next.Mode {
	case config.ModePingAck:
		if modeChanged || d.loopPA == nil {
			d.startProtocol(next)
		} else {
			// live-tune ping/ack
			d.loopPA.Reconfigure(next)
		}
	case config.ModeGossip:
		if modeChanged || d.loopGo == nil {
			d.startProtocol(next)
		} else {
			// live-tune gossip
			d.loopGo.Reconfigure(next)
		}
	}

	// Reconfigure suspicion manager (including gate)
	if d.sm != nil {
		useTO := (next.Mode == config.ModePingAck)
		d.sm.Reconfigure(next.TSuspect, next.TFail, next.TCleanup, useTO)
		if !useTO && d.inflight != nil {
			d.inflight.Reset()
		}
	}

	//Reconfigure store (currently only logging level)
	if d.st != nil {
		d.st.Reconfigure(next)
	}

	// Always refresh per-loop ConfigDTO cache so the *next normal send* piggybacks it.
	dto := next.ToDTO()
	if d.loopGo != nil {
		d.loopGo.SetConfigDTO(dto)
	}
	if d.loopPA != nil {
		d.loopPA.SetConfigDTO(dto)
	}
}

// onConfigFromWire is called by loops when a message with Cfg arrives.
// Treat it as a remote update: apply only if Version is strictly newer.
func (d *Daemon) onConfigFromWire(dto config.ConfigDTO) {
	d.cfgMu.Lock()
	prev := d.cfg
	changed, err := d.cfg.ApplyRemote(dto) // requires dto.Version and > current.Version
	next := d.cfg
	d.cfgMu.Unlock()

	if err != nil || !changed {
		return
	}
	log.Printf(colors.Cyan+"received config change from wire: version %d"+colors.Reset, next.Version)
	d.applyRuntime(prev, next)
}

func (d *Daemon) membershipHTTP(addr string) {

	type apiMember struct {
		ID          string `json:"id"`
		Incarnation uint64 `json:"incarnation"`
		State       string `json:"state"`
		LastUpdate  string `json:"last_update"`
	}

	toAPI := func(list []store.MemberEntry) []apiMember {
		out := make([]apiMember, 0, len(list))
		for _, e := range list {
			out = append(out, apiMember{
				ID:          e.ID.NodeIDToString(),
				Incarnation: e.Incarnation,
				State:       fmt.Sprint(e.State), // relies on fmt for enum; OK for APIs
				LastUpdate:  e.LastUpdate.Format("2006-01-02 15:04:05"),
			})
		}
		return out
	}

	// GET returns the DTO (cluster knobs only)
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		d.cfgMu.RLock()
		cfg := d.cfg // sending entire config object
		d.cfgMu.RUnlock()
		_ = json.NewEncoder(w).Encode(cfg)
	})

	// SET applies local (CLI) updates:
	// bump version locally and rely on loops to piggyback this DTO
	// on their next pingack/gossip.
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		dto, err := loadConfig("../../../config.json") //TODO: change path as needed
		if err != nil {
			http.Error(w, "failed to load config: "+err.Error(), http.StatusInternalServerError)
			log.Printf("admin /set: failed to load config: %v", err)
			return
		}

		d.cfgMu.Lock()
		prev := d.cfg
		changed, err := d.cfg.BumpAndApplyLocal(dto)
		next := d.cfg
		d.cfgMu.Unlock()

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if changed {
			log.Printf(colors.Cyan+"received config change from HTTPServer: version %d"+colors.Reset, next.Version)
			d.applyRuntime(prev, next)
		}
		_ = json.NewEncoder(w).Encode(next)
	})

	http.HandleFunc("/leave", func(w http.ResponseWriter, r *http.Request) {
		membershipList, _ := d.st.Snapshot()
		for _, member := range membershipList {
			if member.ID == d.st.SelfID() {
				selfIncarnationNumber := member.Incarnation
				d.st.ApplyLocked(d.st.SelfID(), selfIncarnationNumber+1, store.StateLeft)
			}
		}
		_ = json.NewEncoder(w).Encode(membershipList)
	})

	// GET /list_mem  -> full membership list (pretty simple JSON)
	http.HandleFunc("/list_mem", func(w http.ResponseWriter, r *http.Request) {
		list, _ := d.st.Snapshot()
		response := map[string]interface{}{
			"members": toAPI(list),
			"count":   len(list),
		}
		_ = json.NewEncoder(w).Encode(response)
	})

	// GET /list_self -> self node id
	http.HandleFunc("/list_self", func(w http.ResponseWriter, r *http.Request) {
		id := d.st.SelfID().NodeIDToString()
		_ = json.NewEncoder(w).Encode(map[string]string{"self_id": id})
	})

	// GET /display_suspects -> only suspected nodes
	http.HandleFunc("/display_suspects", func(w http.ResponseWriter, r *http.Request) {
		list, _ := d.st.Snapshot()
		suspects := make([]store.MemberEntry, 0, len(list))
		for _, e := range list {
			if e.State == store.StateSuspect {
				suspects = append(suspects, e)
			}
		}
		_ = json.NewEncoder(w).Encode(toAPI(suspects))
	})

	// GET /display_protocol -> <{gossip|pingack}, {suspect|nosuspect}>
	http.HandleFunc("/display_protocol", func(w http.ResponseWriter, r *http.Request) {
		d.cfgMu.RLock()
		mode := d.cfg.Mode
		sus := "enabled"
		if d.cfg.TSuspect == 0 {
			sus = "disabled"
		}
		d.cfgMu.RUnlock()
		_ = json.NewEncoder(w).Encode(map[string]string{
			"protocol":  string(mode),
			"suspicion": sus,
		})
	})

	// POST /switch  body: {"protocol":"gossip"|"pingack","suspicion":"suspect"|"nosuspect"}
	type switchReq struct {
		Protocol      string `json:"protocol"`
		Suspicion     string `json:"suspicion"`
		SuspicionTime string `json:"suspicion_time,omitempty"` // optional, e.g. "3ms"
	}

	http.HandleFunc("/switch", func(w http.ResponseWriter, r *http.Request) {
		var req switchReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Build a complete DTO from current config, then tweak just the two fields
		d.cfgMu.RLock()
		dto := d.cfg.ToDTO()
		d.cfgMu.RUnlock()

		// protocol
		//p can be either "gossip" or "pingack"
		p := strings.ToLower(strings.TrimSpace(req.Protocol))
		if p != "gossip" && p != "pingack" {
			http.Error(w, "protocol must be one of: gossip, pingack", http.StatusBadRequest)
			return
		}
		dto.Mode = p

		// suspicion
		s := strings.ToLower(strings.TrimSpace(req.Suspicion))
		t := strings.TrimSpace(req.SuspicionTime)
		log.Printf("admin /switch: protocol=%s suspicion=%s suspicion_time=%s dto_stime=%s", p, s, t, dto.TSuspect)

		switch s {
		case "enabled":
			if t == "" {
				http.Error(w, "suspicion_time is required when enabling suspicion", http.StatusBadRequest)
				return
			}
			if t != "" {
				// parse and override TSuspect if provided
				dur, err := time.ParseDuration(t)
				if err != nil || dur <= 0 {
					http.Error(w, "invalid suspicion_time, must be a positive duration like '3000ms'", http.StatusBadRequest)
					return
				}
				dto.TSuspect = dur.String()
			}

		case "disabled":
			dto.TSuspect = "0ms"
		default:
			http.Error(w, "suspicion must be one of: enabled, disabled", http.StatusBadRequest)
			return
		}

		// Apply locally with version bump; piggyback via the loops
		d.cfgMu.Lock()
		prev := d.cfg
		changed, err := d.cfg.BumpAndApplyLocal(dto)
		next := d.cfg
		d.cfgMu.Unlock()

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if changed {
			log.Printf(colors.Cyan+"switch: protocol=%s suspicion=%s -> version %d"+colors.Reset, dto.Mode, s, next.Version)
			d.applyRuntime(prev, next)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"protocol":       dto.Mode,
			"suspicion":      s,
			"suspicion_time": dto.TSuspect,
			"new_version":    next.Version,
			"effectiveDTO":   next.ToDTO(),
		})
	})

	log.Printf("admin http listening on %s", addr)
	_ = http.ListenAndServe(addr, nil)
}

// introducer TCP server loop which accepts join requests
func (d *Daemon) startIntroducerServer(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen as introducer: %v", err)
	}
	log.Printf("introducer listening on %s", addr)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("accept error: %v", err)
				continue
			}
			go d.handleJoin(conn) // handle each joiner in its own goroutine
		}
	}()
}

func (d *Daemon) handleJoin(conn net.Conn) {
	defer conn.Close()

	var req wire.JoinRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		log.Printf("bad join request: %v", err)
		return
	}

	// withTS := d.st.ConvertFromWithoutTimestamps(req.List)

	// merge membership list from joiner
	d.st.MergeFullList(req.List, "daemon join request - introducer")

	// build response with introducer’s config + updated membership list
	membership, _ := d.st.Snapshot()
	// withoutTS := d.st.ConvertToWithoutTimestamps(membership)

	resp := wire.JoinResponse{
		ConfigDTO: d.cfg.ToDTO(), // introducer’s config always wins
		List:      membership,
	}

	if err := json.NewEncoder(conn).Encode(&resp); err != nil {
		log.Printf("failed to send join response: %v", err)
	}
}

// joiner node TCP client loop which connects to introducer and sends join request
func (d *Daemon) joinCluster(introducerAddr string) {
	for {
		conn, err := net.Dial("tcp", introducerAddr)
		if err != nil {
			log.Printf("failed to connect to introducer %s: %v, retrying...", introducerAddr, err)
			time.Sleep(2 * time.Second)
			continue
		}

		// prepare request with our config + membership
		membership, _ := d.st.Snapshot()
		// withoutTS := d.st.ConvertToWithoutTimestamps(membership)

		req := wire.JoinRequest{
			List: membership,
		}
		if err := json.NewEncoder(conn).Encode(&req); err != nil {
			log.Printf("failed to send join request: %v", err)
			conn.Close()
			continue
		}

		// wait for response
		var resp wire.JoinResponse
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			log.Printf("failed to read join response: %v", err)
			conn.Close()
			continue
		}

		// apply config from introducer (ignore ours)
		d.cfgMu.Lock()
		prev := d.cfg
		changed, err := d.cfg.ApplyRemote(resp.ConfigDTO)
		next := d.cfg
		d.cfgMu.Unlock()
		if err != nil {
			log.Printf("bad config from introducer: %v", err)
		}

		if changed {
			log.Printf(colors.Cyan+"received config change from Introducer: version %d"+colors.Reset, next.Version)
			d.applyRuntime(prev, next)
		}

		// withTS := d.st.ConvertFromWithoutTimestamps(resp.List)
		// merge membership list
		d.st.MergeFullList(resp.List, "daemon join response - new node")

		conn.Close()
		break // success, stop retrying
	}
}

func (d *Daemon) Close() {
	d.stopPingAck()
	d.stopGossip()
	if d.sm != nil {
		d.sm.Close()
	}
	if d.net != nil {
		_ = d.net.Close()
	}
}

func loadConfig(path string) (config.ConfigDTO, error) {
	var dto config.ConfigDTO
	data, err := os.ReadFile(path)
	if err != nil {
		return dto, err
	}
	if err := json.Unmarshal(data, &dto); err != nil {
		return dto, err
	}
	return dto, nil
}

//---------------------------------------------------------------------------------------

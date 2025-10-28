// pkg/protocol/gossip/loop.go
package gossip

import (
	"fmt"
	"hydfs-g33/config"
	"hydfs-g33/membership/protocol/common"
	"hydfs-g33/membership/store"
	"hydfs-g33/membership/transport"
	"hydfs-g33/membership/wire"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Loop struct {
	mu  sync.RWMutex
	St  *store.Store
	Net *transport.UDP

	// timings
	GossipPeriod time.Duration
	GossipFanout int

	DropRateRecv float64

	stop chan struct{}

	// last config to piggyback + callback to apply inbound configs
	cfgDTO   atomic.Value // stores config.ConfigDTO
	OnConfig func(config.ConfigDTO)
}

func New(st *store.Store, net *transport.UDP, gossipPeriod time.Duration, gossipFanout int) *Loop {
	l := &Loop{
		St:           st,
		Net:          net,
		GossipFanout: gossipFanout,
		GossipPeriod: gossipPeriod,
		stop:         make(chan struct{}),
	}
	// initialize with empty DTO; daemon should set a real one shortly
	l.cfgDTO.Store(config.ConfigDTO{})
	return l
}

// Allow daemon to update the DTO we piggyback
func (l *Loop) SetConfigDTO(dto config.ConfigDTO) { l.cfgDTO.Store(dto) }
func (l *Loop) getConfigDTO() (config.ConfigDTO, bool) {
	v := l.cfgDTO.Load()
	if v == nil {
		return config.ConfigDTO{}, false
	}
	dto, ok := v.(config.ConfigDTO)
	return dto, ok
}

// Run starts recv and send loops.
func (l *Loop) Run(recvBuf int) {
	go l.recvLoop(recvBuf)
	go l.gossipLoop()
}

func (l *Loop) Close() { close(l.stop) }

func (l *Loop) recvLoop(recvBuf int) {
	buf := make([]byte, recvBuf)
	for {
		select {
		case <-l.stop:
			return
		default:
		}

		n, _, err := l.Net.Read(buf)
		if err != nil {
			log.Println("read error in gossip recv loop:", err)
			return
		}
		if l.shouldDrop() {
			continue
		}

		msg, err := wire.DecodeMessage(buf[:n])
		if err != nil {
			log.Println("decode error in gossip recv loop:", err)
			continue
		}

		// apply incoming config if present
		if msg.Cfg != nil && l.OnConfig != nil {
			l.OnConfig(*msg.Cfg)
		}

		// close recv loop if self entry state is Left
		membershipList, selfID := l.St.Snapshot()
		for _, member := range membershipList {
			if member.ID == selfID && member.State == store.StateLeft {
				log.Println("self entry is marked Left; exiting gossip recv loop")
				return
			}
		}

		switch msg.Type {
		case wire.MsgGossip, wire.MsgPing, wire.MsgAck: // accepts ping/ack to handle incoming messages during protocol switch
			l.St.MergeFullList(msg.List, "gossip recv loop")
		}
	}
}

func (l *Loop) gossipLoop() {
	l.mu.RLock()
	ticker := time.NewTicker(l.GossipPeriod)
	l.mu.RUnlock()
	defer ticker.Stop()

	lastPeriod := l.GossipPeriod

	// --- simple TX meter (bytes/second) --- //
	rateTick := time.NewTicker(time.Second)
	defer rateTick.Stop()
	var bytesThisSecond int64

	for {
		select {
		case <-l.stop:
			return

		case <-ticker.C:
			// Snapshot config under read lock
			l.mu.RLock()
			period := l.GossipPeriod
			fanout := l.GossipFanout
			l.mu.RUnlock()

			// Do the gossip work
			membershipList, selfID := l.St.Snapshot()
			peers := common.ResolvePeersFromMemberList(membershipList, selfID)

			if len(peers) > 0 {
				perm := rand.Perm(len(peers))
				k := fanout
				if k > len(perm) {
					k = len(perm)
				}

				msg := wire.Message{Type: wire.MsgGossip, List: membershipList}
				if dto, ok := l.getConfigDTO(); ok {
					msg.Cfg = &dto
				}

				if b, err := wire.EncodeMessage(msg); err == nil {

					//log.Printf("[udp] gossip payload=%dB type=%v entries=%d", len(b), msg.Type, len(msg.List))

					for i := 0; i < k; i++ {
						l.St.PrintTable(fmt.Sprint("going to gossip =>", peers[perm[i]]))
						if to, err := net.ResolveUDPAddr("udp", peers[perm[i]]); err == nil {
							_ = l.Net.WriteTo(b, to)
							bytesThisSecond += int64(len(b)) // NEW: count bytes actually sent
						}
					}
				}
			}

			// Check if GossipPeriod changed → reset ticker
			if period != lastPeriod {
				ticker.Stop()
				ticker = time.NewTicker(period)
				lastPeriod = period
			}

		case <-rateTick.C: // NEW: once per second, report and reset
			//log.Printf("[gossip] tx_payload=%d B/s", bytesThisSecond)
			bytesThisSecond = 0
		}
	}
}

func (l *Loop) shouldDrop() bool {
	//log.Printf("Fail: DropRateRecv=%v", l.DropRateRecv)
	return l.DropRateRecv > 0 && rand.Float64() < l.DropRateRecv
}

func (l *Loop) Reconfigure(next config.Config) {
	l.mu.Lock()
	l.GossipPeriod = next.GossipPeriod
	l.GossipFanout = next.GossipFanout
	l.DropRateRecv = next.DropRateRecv
	l.mu.Unlock()
}

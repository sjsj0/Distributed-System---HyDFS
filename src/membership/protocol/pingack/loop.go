// pkg/protocol/pingack/loop.go
package pingack

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
	PingEvery  time.Duration
	TSuspect   time.Duration
	PingFanout int

	DropRateRecv float64 // simulate receiver-side drop

	// internal
	Inflight *common.Inflight
	stop     chan struct{}

	// latest config to piggyback (set by daemon) + callback to apply inbound configs
	cfgDTO   atomic.Value           // stores config.ConfigDTO
	OnConfig func(config.ConfigDTO) // callback function to apply inbound config
}

func New(st *store.Store, net *transport.UDP, pingEvery, tSuspect time.Duration, pingFanout int) *Loop {
	l := &Loop{
		St:         st,
		Net:        net,
		Inflight:   common.NewInflight(),
		PingEvery:  pingEvery,
		PingFanout: pingFanout,
		TSuspect:   tSuspect,
		stop:       make(chan struct{}),
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
	go l.pingLoop()
}

func (l *Loop) Close() { close(l.stop) }

func (l *Loop) recvLoop(recvBuf int) {
	buf := make([]byte, recvBuf)

	// per-second accounting for ACK bytes we SEND from this loop
	var ackBytesThisSec int
	lastAckLog := time.Now()

	for {
		select {
		case <-l.stop:
			return
		default:
		}

		n, from, err := l.Net.Read(buf) // keeping a note of 'from' for replies
		if err != nil {
			log.Println("read error in pingack recv loop:", err)
			return
		}
		if l.shouldDrop() {
			continue
		}

		msg, err := wire.DecodeMessage(buf[:n])
		if err != nil {
			log.Println("decode error in pingack recv loop:", err)
			continue
		}

		// apply incoming config if present
		if msg.Cfg != nil && l.OnConfig != nil {
			l.OnConfig(*msg.Cfg)
		}

		// TODO: log who sent me ping or ack
		// withTS := l.St.ConvertFromWithoutTimestamps(msg.List)

		// close recv loop if self entry state is Left
		membershipList, selfID := l.St.Snapshot()
		for _, member := range membershipList {
			if member.ID == selfID && member.State == store.StateLeft {
				log.Println("self entry is marked Left; exiting pingack recv loop")
				return
			}
		}

		switch msg.Type {
		case wire.MsgPing:
			// merge sender's view then reply ACK (echo nonce)
			l.St.MergeFullList(msg.List, "pingack recv loop - before sending ack")

			// prepare reply with our current view and piggyback config
			membershipList, _ := l.St.Snapshot() //Mark its own timestamp as time.Now() before sending ack
			// withoutTS := l.St.ConvertToWithoutTimestamps(membershipList)
			reply := wire.Message{Type: wire.MsgAck, Nonce: msg.Nonce, List: membershipList}
			if dto, ok := l.getConfigDTO(); ok {
				reply.Cfg = &dto
			}
			if b, err := wire.EncodeMessage(reply); err == nil {

				//log.Printf("[udp] ack payload=%dB type=%v entries=%d", len(b), msg.Type, len(msg.List))
				_ = l.Net.WriteTo(b, from)

				// account & log per second
				ackBytesThisSec += len(b)
				if time.Since(lastAckLog) >= time.Second {
					//log.Printf("[udp] ack throughput: %d B/s", ackBytesThisSec)
					ackBytesThisSec = 0
					lastAckLog = time.Now()
				}
			}

		case wire.MsgAck:
			_ = l.Inflight.Done(msg.Nonce)
			l.St.MergeFullList(msg.List, "pingack recv loop - on receiving ack")

		case wire.MsgGossip:
			// accept gossip too if mixed protocols
			l.St.MergeFullList(msg.List, "pingack recv loop - on receiving gossip")
		}
	}
}

func (l *Loop) pingLoop() {
	l.mu.RLock()
	ticker := time.NewTicker(l.PingEvery)
	l.mu.RUnlock()
	defer ticker.Stop()

	lastPeriod := l.PingEvery

	// per-second accounting for PING bytes we SEND from this loop
	var pingBytesThisSec int
	lastPingLog := time.Now()

	for {
		select {
		case <-l.stop:
			return

		case <-ticker.C:
			// snapshot config under lock
			l.mu.RLock()
			period := l.PingEvery
			fanout := l.PingFanout
			tSuspect := l.TSuspect
			l.mu.RUnlock()

			membershipList, selfID := l.St.Snapshot() //mark its own timestamp as time.Now() before sending ping
			peers := common.ResolvePeersFromMemberList(membershipList, selfID)
			// withoutTS := l.St.ConvertToWithoutTimestamps(membershipList)

			if len(peers) > 0 {
				perm := rand.Perm(len(peers))
				k := fanout
				if k > len(perm) {
					k = len(perm)
				}

				for i := 0; i < k; i++ {
					addr := peers[perm[i]]
					nonce := rand.Uint64()
					ping := wire.Message{Type: wire.MsgPing, Nonce: nonce, List: membershipList}
					if dto, ok := l.getConfigDTO(); ok {
						ping.Cfg = &dto
					}

					l.St.PrintTable(fmt.Sprint("going to ping =>", addr))
					if b, err := wire.EncodeMessage(ping); err == nil {

						// payload length (exact)
						//log.Printf("[udp] ping payload=%dB type=%v entries=%d", len(b), ping.Type, len(ping.List))

						if to, err := net.ResolveUDPAddr("udp", addr); err == nil {
							_ = l.Net.WriteTo(b, to)
							// Track this ping for this target addr
							l.Inflight.Add(nonce, tSuspect, addr)

							// account & log per second
							pingBytesThisSec += len(b)
							if time.Since(lastPingLog) >= time.Second {
								//log.Printf("[udp] ping throughput: %d B/s", pingBytesThisSec)
								pingBytesThisSec = 0
								lastPingLog = time.Now()
							}
						}
					}
				}
			}

			// reset ticker if PingEvery changed
			if period != lastPeriod {
				ticker.Stop()
				ticker = time.NewTicker(period)
				lastPeriod = period
			}
		}
	}
}

func (l *Loop) shouldDrop() bool {
	//log.Printf("Fail: DropRateRecv=%v", l.DropRateRecv)
	return l.DropRateRecv > 0 && rand.Float64() < l.DropRateRecv
}

func (l *Loop) Reconfigure(next config.Config) {
	l.mu.Lock()
	l.PingEvery = next.PingEvery
	l.TSuspect = next.TSuspect
	l.DropRateRecv = next.DropRateRecv
	l.PingFanout = next.PingFanout
	l.mu.Unlock()
}

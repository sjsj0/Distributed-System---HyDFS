package wire

import (
	"encoding/json"
	"hydfs-g33/config"
	"hydfs-g33/membership/store"
)

type MsgType uint8

const (
	MsgGossip MsgType = 1
	MsgPing   MsgType = 2
	MsgAck    MsgType = 3
)

type Message struct {
	Type  MsgType             `json:"type"`
	Nonce uint64              `json:"nonce,omitempty"`
	List  []store.MemberEntry `json:"list"`
	Cfg   *config.ConfigDTO   `json:"cfg,omitempty"`
}

type JoinRequest struct {
	List []store.MemberEntry `json:"list"`
}

type JoinResponse struct {
	ConfigDTO config.ConfigDTO    `json:"config"`
	List      []store.MemberEntry `json:"list"`
}

func EncodeMessage(m Message) ([]byte, error) {
	return json.Marshal(m)
}

func DecodeMessage(b []byte) (Message, error) {
	var m Message
	err := json.Unmarshal(b, &m)
	return m, err
}

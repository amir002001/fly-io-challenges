package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessageBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type ReadMessagReply struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type TopologyMessageBody struct {
	Type     string   `json:"type"`
	Topology Topology `json:"topology"`
}

type Topology map[string][]string

type Messages struct {
	Mu          sync.RWMutex
	MessageList []int
	MessageSet  map[int]struct{}
}

type WriteMessageBody BroadcastMessageBody

func main() {
	// this version is best effort
	n := maelstrom.NewNode()
	messages := Messages{
		Mu:          sync.RWMutex{},
		MessageList: []int{},
		MessageSet:  make(map[int]struct{}),
	}

	var topology Topology

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcastMessageBody BroadcastMessageBody
		reply := map[string]string{"type": "broadcast_ok"}
		err := json.Unmarshal(msg.Body, &broadcastMessageBody)
		if err != nil {
			return err
		}
		messages.Mu.RLock()
		_, ok := messages.MessageSet[broadcastMessageBody.Message]
		messages.Mu.RUnlock()
		if ok {
			return n.Reply(msg, maelstrom.KeyAlreadyExists)
		}
		messages.Mu.Lock()
		messages.MessageSet[broadcastMessageBody.Message] = struct{}{}
		messages.MessageList = append(messages.MessageList, broadcastMessageBody.Message)
		messages.Mu.Unlock()

		for _, node := range topology[n.ID()] {
			for {
				rpcMessage, err := n.SyncRPC(context.Background(), node, broadcastMessageBody)
				if err == nil {
					break
				}
			}
		}

		return n.Reply(msg, reply)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		messages.Mu.RLock()
		reply := ReadMessagReply{
			Type:     "read_ok",
			Messages: messages.MessageList,
		}
		messages.Mu.RUnlock()

		return n.Reply(msg, reply)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var topologyMessageBody TopologyMessageBody
		err := json.Unmarshal(msg.Body, &topologyMessageBody)
		if err != nil {
			return err
		}
		topology = topologyMessageBody.Topology

		reply := map[string]string{"type": "topology_ok"}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

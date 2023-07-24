package main

import (
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

type MessageList struct {
	Mu       sync.RWMutex
	Messages []int
}

type WriteMessageBody BroadcastMessageBody

func main() {
	// this version is best effort
	n := maelstrom.NewNode()
	messageList := MessageList{
		Mu:       sync.RWMutex{},
		Messages: []int{},
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcastMessageBody BroadcastMessageBody
		err := json.Unmarshal(msg.Body, &broadcastMessageBody)
		if err != nil {
			return err
		}
		for _, node := range n.NodeIDs() {
			writeMessage := WriteMessageBody{
				Type:    "write",
				Message: broadcastMessageBody.Message,
			}
			err := n.Send(node, writeMessage)
			if err != nil {
				return err
			}
		}
		reply := map[string]string{"type": "broadcast_ok"}
		return n.Reply(msg, reply)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		messageList.Mu.RLock()
		reply := ReadMessagReply{
			Type:     "read_ok",
			Messages: messageList.Messages,
		}
		messageList.Mu.RUnlock()

		return n.Reply(msg, reply)
	})

	n.Handle("write", func(msg maelstrom.Message) error {
		var writeMessageBody WriteMessageBody
		err := json.Unmarshal(msg.Body, &writeMessageBody)
		if err != nil {
			return err
		}
		messageList.Mu.Lock()
		messageList.Messages = append(messageList.Messages, writeMessageBody.Message)
		messageList.Mu.Unlock()

		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		reply := map[string]string{"type": "topology_ok"}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessageBody struct {
	MessageType string `json:"type"`
	Message     int    `json:"message"`
}

func main() {
	n := maelstrom.NewNode()

	messageMap := make(
		map[int]struct{},
	) // if it's accidentally sent twice? gotta make sure idempotent

	messageList := make(
		[]int, 0,
	)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if _, ok := messageMap[body.Message]; !ok {
			messageMap[body.Message] = struct{}{}
			messageList = append(messageList, body.Message)

			for _, node := range n.NodeIDs() {
				err := n.Send(node, body)
				if err != nil {
					return err
				}
			}
		}

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{
			"type":     "read_ok",
			"messages": messageList,
		}
		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// ignore for now
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

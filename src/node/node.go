package node

import "github.com/gorilla/websocket"

type Client struct {
	ID   string
	Name string
	Conn *websocket.Conn
	Send chan []byte
	IsMaster bool
}

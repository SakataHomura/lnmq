package qlookup

import "github.com/lnmq/qnet"

type TcpClient struct {
	peerInfo *PeerInfo

	tcpConn  *qnet.TcpConnect
	protocol *Protocol
}

//go:build unix

package worker

import (
	"errors"
	"net"
	"syscall"
)

// connAlive reports whether the peer has not closed c, without blocking or
// consuming data. Before a request is sent the server has nothing to say,
// so a non-blocking peek returning EAGAIN means open; EOF, data or any
// error means the connection must not be used.
func connAlive(c net.Conn) bool {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	alive := false
	err = raw.Read(func(fd uintptr) bool {
		var b [1]byte
		_, _, rerr := syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		alive = errors.Is(rerr, syscall.EAGAIN) || errors.Is(rerr, syscall.EWOULDBLOCK)
		return true // never wait for readiness
	})
	return err == nil && alive
}

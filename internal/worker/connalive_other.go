//go:build !unix

package worker

import "net"

// connAlive cannot check without blocking here, so it treats every warm
// connection as possibly closed: they are discarded and requests dial anew.
func connAlive(net.Conn) bool { return false }

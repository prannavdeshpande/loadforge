package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/transport"
)

// fanoutResult is what a command's scatter-gather collected.
type fanoutResult struct {
	acks    map[string]transport.Ack // first ack from each member
	missing []string                 // members with no ack at the deadline, in member order
}

// fanout publishes cmd on the run's command subject and collects one ack
// from each member until all have acked, the deadline (monotonic) passes,
// ctx ends, or onAck asks to stop. Every member subscribes to the same
// subject, so a re-send reaches all of them: it reuses the cmd_seq, and a
// worker that already answered replays its cached ack, which is dropped
// here as a duplicate. onAck runs on the calling goroutine.
func fanout(ctx context.Context, nc *nats.Conn, cfg Config, cmd transport.Command, members []string,
	deadline time.Time, onAck func(transport.Ack) (stop bool)) (fanoutResult, error) {
	res := fanoutResult{acks: map[string]transport.Ack{}}
	member := make(map[string]bool, len(members))
	for _, m := range members {
		member[m] = true
	}
	finish := func() {
		for _, m := range members {
			if _, ok := res.acks[m]; !ok {
				res.missing = append(res.missing, m)
			}
		}
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return res, err
	}
	inbox := nc.NewRespInbox()
	ch := make(chan *nats.Msg, 4*len(members)+16)
	sub, err := nc.ChanSubscribe(inbox, ch)
	if err != nil {
		return res, err
	}
	defer func() { _ = sub.Unsubscribe() }()

	subject := transport.CmdSubject(cmd.RunID)
	send := func() {
		if err := nc.PublishRequest(subject, inbox, data); err != nil {
			log.Printf("controlplane: run %s: send %s seq %d: %v", cmd.RunID, cmd.Type, cmd.CmdSeq, err)
		}
	}
	send()

	wait := time.Until(deadline)
	if wait < 0 {
		wait = 0
	}
	dl := time.NewTimer(wait)
	defer dl.Stop()
	backoff := cfg.RetryInterval
	retry := time.NewTimer(backoff)
	defer retry.Stop()

	for len(res.acks) < len(members) {
		select {
		case <-ctx.Done():
			finish()
			return res, context.Cause(ctx)
		case <-dl.C:
			finish()
			return res, nil
		case <-retry.C:
			send()
			backoff = min(2*backoff, cfg.MaxRetryInterval)
			retry.Reset(backoff)
		case m := <-ch:
			var a transport.Ack
			if err := json.Unmarshal(m.Data, &a); err != nil {
				log.Printf("controlplane: run %s: undecodable ack: %v", cmd.RunID, err)
				continue
			}
			if a.RunID != cmd.RunID || a.CmdSeq != cmd.CmdSeq {
				log.Printf("controlplane: run %s: ack for run %q seq %d ignored (want seq %d)", cmd.RunID, a.RunID, a.CmdSeq, cmd.CmdSeq)
				continue
			}
			if !member[a.WorkerID] {
				log.Printf("controlplane: run %s: ack from non-member %q ignored", cmd.RunID, a.WorkerID)
				continue
			}
			if _, dup := res.acks[a.WorkerID]; dup {
				continue
			}
			res.acks[a.WorkerID] = a
			if onAck != nil && onAck(a) {
				finish()
				return res, nil
			}
		}
	}
	return res, nil
}

func describeMissing(cmd transport.CmdType, missing []string, d time.Duration) string {
	return fmt.Sprintf("no %s ack from %v within %v", cmd, missing, d.Round(time.Millisecond))
}

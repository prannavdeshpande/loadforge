package controlplane

import (
	"context"
	"log"
	"sync"
	"time"
)

// leaseKeeper renews one run lease and decides whether heartbeats may be
// published. All times are monotonic.
type leaseKeeper struct {
	cfg    Config
	leaser Leaser
	key    string
	token  uint64

	mu        sync.Mutex
	confirmed time.Time // when the last successful acquire or renew was sent
	lost      bool

	lostCh chan struct{}
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
}

// newLeaseKeeper starts renewing a lease that was acquired by a call sent
// at acquiredAt. The keeper outlives ctx (a request's, say): it uses ctx
// only for its values.
func newLeaseKeeper(ctx context.Context, cfg Config, leaser Leaser, key string, token uint64, acquiredAt time.Time) *leaseKeeper {
	k := &leaseKeeper{
		cfg: cfg, leaser: leaser, key: key, token: token, confirmed: acquiredAt,
		lostCh: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go k.loop(context.WithoutCancel(ctx))
	return k
}

func (k *leaseKeeper) loop(ctx context.Context) {
	defer close(k.done)
	t := time.NewTicker(k.cfg.RenewInterval)
	defer t.Stop()
	for {
		select {
		case <-k.stop:
			return
		case <-t.C:
		}
		sent := time.Now()
		rctx, cancel := context.WithTimeout(ctx, k.cfg.RenewInterval)
		ok, err := k.leaser.Renew(rctx, k.key, k.token)
		cancel()
		switch {
		case err != nil:
			// Redis unreachable: the lease may still be valid, but the gate
			// closes on its own once the last confirmation is too old, and
			// once a whole TTL has passed it has certainly expired.
			log.Printf("controlplane: renew %s: %v", k.key, err)
			k.mu.Lock()
			expired := time.Since(k.confirmed) >= k.cfg.LeaseTTL
			k.mu.Unlock()
			if expired {
				k.markLost("lease expired while Redis was unreachable")
				return
			}
		case !ok:
			k.markLost("renewal refused: the lease expired or another controller holds it")
			return
		default:
			k.mu.Lock()
			k.confirmed = sent
			k.mu.Unlock()
		}
	}
}

func (k *leaseKeeper) markLost(why string) {
	k.mu.Lock()
	already := k.lost
	k.lost = true
	k.mu.Unlock()
	if !already {
		log.Printf("controlplane: lease %s (token %d) lost: %s", k.key, k.token, why)
		close(k.lostCh)
	}
}

// gateOpen reports whether a heartbeat may be published now (see
// Config.HeartbeatGate).
func (k *leaseKeeper) gateOpen() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return !k.lost && time.Since(k.confirmed) < k.cfg.HeartbeatGate()
}

// Lost is closed once the lease is known to be lost.
func (k *leaseKeeper) Lost() <-chan struct{} { return k.lostCh }

// close stops renewing and, if release is set, deletes the lease if this
// token still holds it.
func (k *leaseKeeper) close(ctx context.Context, release bool) {
	k.once.Do(func() {
		close(k.stop)
		<-k.done
		if !release {
			return
		}
		ctx, cancel := context.WithTimeout(ctx, k.cfg.StoreTimeout)
		defer cancel()
		if _, err := k.leaser.Release(ctx, k.key, k.token); err != nil {
			log.Printf("controlplane: release %s: %v", k.key, err)
		}
	})
}

// abandon marks the lease lost (so no more heartbeats are gated open) and
// releases it, for a holder that must give up the run at once.
func (k *leaseKeeper) abandon(ctx context.Context, why string) {
	k.markLost(why)
	k.close(ctx, true)
}

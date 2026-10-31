package sim

import "time"

// NetConfig sets how faulty the network is. Probabilities are per message.
type NetConfig struct {
	// DropP is the chance a message is lost. DupP is the chance a message
	// is delivered a second time, and applies only to messages the sender
	// marked idempotent, because only those are safe to repeat.
	DropP, DupP float64
	// Latency is the range of typical one-way latency.
	Latency [2]time.Duration
	// TailP is the chance a message takes a tail latency instead, drawn
	// from Tail. A scenario should set the upper end of Tail above the
	// worker margin (3s by default) on purpose, so that some messages
	// arrive too late to be useful.
	TailP float64
	Tail  [2]time.Duration
}

// NetStats counts what the network did. Every Send counts once in Sent, and
// a message that is not delivered counts in Dropped or Partitioned, never
// both. Duplicated counts the extra deliveries.
type NetStats struct {
	Sent, Dropped, Duplicated, Partitioned int
}

// link is an unordered pair of nodes, stored with the smaller id first.
type link struct{ a, b NodeID }

func linkOf(a, b NodeID) link {
	if b < a {
		a, b = b, a
	}
	return link{a, b}
}

// Network delivers messages between nodes by scheduling events in a World,
// and takes every random decision from the world's Rand.
type Network struct {
	w          *World
	cfg        NetConfig
	partitions map[link]time.Duration // until, exclusive
	stats      NetStats
	// brokers are the broker nodes attached to the network, by id. A client
	// knows a broker only by its NodeID, and finds the node here when a
	// message reaches it.
	brokers map[NodeID]*BrokerNode
}

// NewNetwork returns a network over w. It panics if a latency range is
// negative or inverted, since that would fail on the first message instead
// of at the line that configured it.
func NewNetwork(w *World, cfg NetConfig) *Network {
	for _, r := range [][2]time.Duration{cfg.Latency, cfg.Tail} {
		if r[0] < 0 || r[1] < r[0] {
			panic("sim: NewNetwork called with a negative or inverted latency range")
		}
	}
	return &Network{w: w, cfg: cfg, partitions: map[link]time.Duration{}, brokers: map[NodeID]*BrokerNode{}}
}

// Send sends one message from one node to another. The network decides, in
// this order: whether a partition blocks the pair, whether the message is
// dropped, how long it takes, and, for an idempotent message, whether it is
// delivered twice. deliver runs once per delivery, as an event labelled
// label (the extra delivery is labelled with " (dup)" appended).
//
// A partition and a drop lose the message silently, as a real network does.
func (n *Network) Send(from, to NodeID, idempotent bool, label string, deliver func()) {
	n.stats.Sent++
	if until, ok := n.partitions[linkOf(from, to)]; ok && n.w.Now < until {
		n.stats.Partitioned++
		return
	}
	if n.w.Rand.Chance(n.cfg.DropP) {
		n.stats.Dropped++
		return
	}
	n.w.After(n.latency(), label, deliver)
	if idempotent && n.w.Rand.Chance(n.cfg.DupP) {
		n.stats.Duplicated++
		n.w.After(n.latency(), label+" (dup)", deliver)
	}
}

// latency draws one message's one-way latency.
func (n *Network) latency() time.Duration {
	if n.w.Rand.Chance(n.cfg.TailP) {
		return n.w.Rand.Between(n.cfg.Tail[0], n.cfg.Tail[1])
	}
	return n.w.Rand.Between(n.cfg.Latency[0], n.cfg.Latency[1])
}

// Partition blocks both directions between a and b until virtual time until:
// a message sent while Now < until is dropped, and one sent at until or later
// is not. A message already in flight is still delivered. Partitioning a
// pair that is already partitioned keeps the later of the two ends.
func (n *Network) Partition(a, b NodeID, until time.Duration) {
	k := linkOf(a, b)
	n.partitions[k] = max(n.partitions[k], until)
}

// Heal starts the heal phase: it lifts every partition and sets DropP, DupP
// and TailP to zero, so that a message sent from now on is delivered once,
// within the typical latency, unless a later Partition blocks it. It does
// not bring back messages already lost.
func (n *Network) Heal() {
	clear(n.partitions)
	n.cfg.DropP, n.cfg.DupP, n.cfg.TailP = 0, 0, 0
}

// Stats returns the counters so far.
func (n *Network) Stats() NetStats {
	return n.stats
}

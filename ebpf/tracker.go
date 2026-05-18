//go:build linux

// Package ebpf provides the Go-side driver for the TC eBPF program compiled
// from tc_tracker.c. The EBPFTracker lives here — not in internal/metrics —
// because bpf2go generates bpfObjects and loadBpfObjects as unexported types
// in this package; they cannot be referenced from any other package.
package ebpf

import (
	"fmt"
	"net"
	"os"

	cebpf "github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"go.uber.org/zap"
)

// rttSlowThreshold is the TCP RTT (µs) above which the backend is considered
// congested and the prefetch look-ahead should be widened.
const rttSlowThreshold = 50_000 // 50 ms

// EBPFTracker attaches the TC eBPF program and exposes live kernel metrics.
type EBPFTracker struct {
	objs    bpfObjects
	tcLink  link.Link
	ifIndex int
	log     *zap.Logger
}

// NewEBPFTracker loads the compiled eBPF bytecode and attaches it to the
// egress TC hook of the named network interface (e.g., "eth0").
// Call Close to detach the program on shutdown.
func NewEBPFTracker(ifaceName string, log *zap.Logger) (*EBPFTracker, error) {
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return nil, fmt.Errorf("ebpf: resolve interface %q: %w", ifaceName, err)
	}

	var objs bpfObjects
	if err := loadBpfObjects(&objs, &cebpf.CollectionOptions{}); err != nil {
		return nil, fmt.Errorf("ebpf: load objects: %w", err)
	}

	tcLink, err := link.AttachTCX(link.TCXOptions{
		Interface: iface.Index,
		Program:   objs.TcTracker,
		Attach:    cebpf.AttachTCXEgress,
	})
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("ebpf: attach TC egress on %q: %w", ifaceName, err)
	}

	log.Info("eBPF TC tracker attached",
		zap.String("interface", ifaceName),
		zap.Int("ifindex", iface.Index),
	)

	return &EBPFTracker{objs: objs, tcLink: tcLink, ifIndex: iface.Index, log: log}, nil
}

// RTTMicros returns the most recent smoothed RTT sample (µs) for the given
// backend address, or 0 if no sample has been recorded yet.
func (t *EBPFTracker) RTTMicros(backendAddr [4]byte) (uint32, error) {
	var val uint32
	if err := t.objs.RttSamples.Lookup(backendAddr, &val); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("ebpf: RTT lookup: %w", err)
	}
	return val, nil
}

// IsSlow returns true if the latest recorded RTT for backendAddr exceeds
// rttSlowThreshold, indicating the link is congested.
func (t *EBPFTracker) IsSlow(backendAddr [4]byte) bool {
	rtt, err := t.RTTMicros(backendAddr)
	if err != nil {
		return false
	}
	return rtt > rttSlowThreshold
}

// Close detaches the TC program and releases all kernel resources.
func (t *EBPFTracker) Close() error {
	if err := t.tcLink.Close(); err != nil {
		return fmt.Errorf("ebpf: close TC link: %w", err)
	}
	return t.objs.Close()
}

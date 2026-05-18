//go:build linux

package ebpf

import (
	"fmt"
	"net"
	"os"

	cebpf "github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"go.uber.org/zap"
)

const rttSlowThreshold = 50_000

type EBPFTracker struct {
	objs    bpfObjects
	tcLink  link.Link
	ifIndex int
	log     *zap.Logger
}

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

func (t *EBPFTracker) IsSlow(backendAddr [4]byte) bool {
	rtt, err := t.RTTMicros(backendAddr)
	if err != nil {
		return false
	}
	return rtt > rttSlowThreshold
}

func (t *EBPFTracker) Close() error {
	if err := t.tcLink.Close(); err != nil {
		return fmt.Errorf("ebpf: close TC link: %w", err)
	}
	return t.objs.Close()
}

//go:build linux

package ebpf

import (
	"bytes"
	_ "embed"
	"fmt"
	"io"

	"github.com/cilium/ebpf"
)

//go:embed tc_tracker.bpf.o
var _BpfBytes []byte

type bpfSpecs struct {
	bpfProgramSpecs
	bpfMapSpecs
}

type bpfProgramSpecs struct {
	TcTracker *ebpf.ProgramSpec `ebpf:"tc_tracker"`
}

type bpfMapSpecs struct {
	RttSamples  *ebpf.MapSpec `ebpf:"rtt_samples"`
	BytesTx     *ebpf.MapSpec `ebpf:"bytes_tx"`
	Retransmits *ebpf.MapSpec `ebpf:"retransmits"`
}

type bpfObjects struct {
	bpfPrograms
	bpfMaps
}

func (o *bpfObjects) Close() error {
	return _BpfClose(
		&o.bpfPrograms,
		&o.bpfMaps,
	)
}

type bpfPrograms struct {
	TcTracker *ebpf.Program `ebpf:"tc_tracker"`
}

func (p *bpfPrograms) Close() error {
	return _BpfClose(p.TcTracker)
}

type bpfMaps struct {
	RttSamples  *ebpf.Map `ebpf:"rtt_samples"`
	BytesTx     *ebpf.Map `ebpf:"bytes_tx"`
	Retransmits *ebpf.Map `ebpf:"retransmits"`
}

func (m *bpfMaps) Close() error {
	return _BpfClose(m.RttSamples, m.BytesTx, m.Retransmits)
}

func loadBpfObjects(objs *bpfObjects, opts *ebpf.CollectionOptions) error {
	spec, err := loadBpf()
	if err != nil {
		return err
	}
	return spec.LoadAndAssign(objs, opts)
}

func loadBpf() (*ebpf.CollectionSpec, error) {
	return loadBpfFromReader(bytes.NewReader(_BpfBytes))
}

func loadBpfFromReader(rd io.ReaderAt) (*ebpf.CollectionSpec, error) {
	spec, err := ebpf.LoadCollectionSpecFromReader(rd)
	if err != nil {
		return nil, fmt.Errorf("can't load bpf: %w", err)
	}
	return spec, nil
}

func _BpfClose(closers ...io.Closer) error {
	for _, c := range closers {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

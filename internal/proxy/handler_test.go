package proxy

import "testing"

func TestResolveByteRange(t *testing.T) {
	const size = 1000
	tests := []struct {
		header     string
		start, end int64
		ok         bool
	}{
		{"bytes=0-99", 0, 99, true},
		{"bytes=900-1999", 900, 999, true}, // clamped to object end
		{"bytes=500-", 500, 999, true},     // open-ended
		{"bytes=-100", 900, 999, true},     // suffix
		{"bytes=-5000", 0, 999, true},      // suffix longer than object
		{"bytes=1000-", 0, 0, false},       // starts past end
		{"bytes=-0", 0, 0, false},
		{"bytes=50-10", 0, 0, false},
		{"items=0-10", 0, 0, false},
		{"bytes=abc", 0, 0, false},
	}
	for _, tt := range tests {
		start, end, ok := resolveByteRange(tt.header, size)
		if ok != tt.ok || (ok && (start != tt.start || end != tt.end)) {
			t.Errorf("%q: got (%d, %d, %v), want (%d, %d, %v)", tt.header, start, end, ok, tt.start, tt.end, tt.ok)
		}
	}
}

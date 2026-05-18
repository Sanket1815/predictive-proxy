// tc_tracker.c — BPF Traffic Control classifier for the predictive proxy.
//
// Compile with:
//   clang -O2 -g -target bpf -D__TARGET_ARCH_x86 \
//         -I/usr/include/x86_64-linux-gnu \
//         -c tc_tracker.c -o tc_tracker.bpf.o
//   llvm-strip -g tc_tracker.bpf.o
//
// Then regenerate Go bindings:
//   go generate ./ebpf/...
//
// This program attaches to the TC egress hook and records:
//   1. Per-backend-IP TCP RTT samples (from tcp_skinfo)
//   2. Bytes transmitted per 5-tuple (for bandwidth accounting)
//   3. Retransmission counts per backend IP (early congestion detection)
//
// All maps are BPF_MAP_TYPE_LRU_HASH with a 1024-entry cap to bound kernel
// memory regardless of backend connection fan-out.

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -cc clang -cflags "-O2 -g -Wall" bpf tc_tracker.c -- -I/usr/include

#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/tcp.h>
#include <linux/ip.h>
#include <linux/in.h>
#include <linux/if_ether.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <bpf/bpf_tracing.h>

// ── Map definitions ──────────────────────────────────────────────────────────

// rtt_samples: backend IPv4 → latest RTT sample in microseconds.
// Key:   __u32 (big-endian IPv4 address of the S3/Wasabi endpoint)
// Value: __u32 (RTT in µs, written by the tcp_skinfo helper)
struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 1024);
    __type(key,         __u32);
    __type(value,       __u32);
} rtt_samples SEC(".maps");

// bytes_tx: backend IPv4 → total egress bytes to that host.
struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 1024);
    __type(key,         __u32);
    __type(value,       __u64);
} bytes_tx SEC(".maps");

// retransmits: backend IPv4 → cumulative retransmit count.
struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 1024);
    __type(key,         __u32);
    __type(value,       __u32);
} retransmits SEC(".maps");

// ── Helper: safe pointer arithmetic within BPF verifier bounds ──────────────

static __always_inline int parse_ip_dst(struct __sk_buff *skb, __u32 *dst_ip) {
    void *data     = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return -1;
    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return -1;

    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end)
        return -1;
    if (iph->protocol != IPPROTO_TCP)
        return -1;

    *dst_ip = iph->daddr; // big-endian, matches bpf_tcp_sock dst
    return 0;
}

// ── Main TC egress program ───────────────────────────────────────────────────

SEC("tc")
int tc_tracker(struct __sk_buff *skb) {
    __u32 dst_ip = 0;
    if (parse_ip_dst(skb, &dst_ip) < 0)
        return TC_ACT_OK; // not a TCP/IP packet; pass through unmodified

    // ── Bytes transmitted ─────────────────────────────────────────────────
    __u64 pkt_len  = skb->len;
    __u64 *tx_bytes = bpf_map_lookup_elem(&bytes_tx, &dst_ip);
    if (tx_bytes) {
        __sync_fetch_and_add(tx_bytes, pkt_len);
    } else {
        bpf_map_update_elem(&bytes_tx, &dst_ip, &pkt_len, BPF_ANY);
    }

    // ── RTT sample via bpf_tcp_sock ───────────────────────────────────────
    struct bpf_tcp_sock *tcp = bpf_tcp_sock(bpf_sk_fullsock(skb->sk));
    if (tcp) {
        __u32 rtt_us = tcp->srtt_us >> 3; // Linux stores srtt as 8× smoothed RTT
        bpf_map_update_elem(&rtt_samples, &dst_ip, &rtt_us, BPF_ANY);

        // ── Retransmission counter ────────────────────────────────────────
        __u32 retrans = tcp->total_retrans;
        __u32 *prev = bpf_map_lookup_elem(&retransmits, &dst_ip);
        if (prev) {
            if (retrans > *prev)
                __sync_fetch_and_add(prev, retrans - *prev);
        } else {
            bpf_map_update_elem(&retransmits, &dst_ip, &retrans, BPF_ANY);
        }
    }

    return TC_ACT_OK; // always pass — we observe only, never drop
}

char LICENSE[] SEC("license") = "GPL";

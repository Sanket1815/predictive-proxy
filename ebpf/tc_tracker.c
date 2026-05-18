#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/tcp.h>
#include <linux/ip.h>
#include <linux/in.h>
#include <linux/if_ether.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <bpf/bpf_tracing.h>

struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 1024);
    __type(key,         __u32);
    __type(value,       __u32);
} rtt_samples SEC(".maps");

struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 1024);
    __type(key,         __u32);
    __type(value,       __u64);
} bytes_tx SEC(".maps");

struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 1024);
    __type(key,         __u32);
    __type(value,       __u32);
} retransmits SEC(".maps");

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

    *dst_ip = iph->daddr;
    return 0;
}

SEC("tc")
int tc_tracker(struct __sk_buff *skb) {
    __u32 dst_ip = 0;
    if (parse_ip_dst(skb, &dst_ip) < 0)
        return TC_ACT_OK;

    __u64 pkt_len  = skb->len;
    __u64 *tx_bytes = bpf_map_lookup_elem(&bytes_tx, &dst_ip);
    if (tx_bytes) {
        __sync_fetch_and_add(tx_bytes, pkt_len);
    } else {
        bpf_map_update_elem(&bytes_tx, &dst_ip, &pkt_len, BPF_ANY);
    }

    struct bpf_tcp_sock *tcp = bpf_tcp_sock(bpf_sk_fullsock(skb->sk));
    if (tcp) {
        __u32 rtt_us = tcp->srtt_us >> 3;
        bpf_map_update_elem(&rtt_samples, &dst_ip, &rtt_us, BPF_ANY);

        __u32 retrans = tcp->total_retrans;
        __u32 *prev = bpf_map_lookup_elem(&retransmits, &dst_ip);
        if (prev) {
            if (retrans > *prev)
                __sync_fetch_and_add(prev, retrans - *prev);
        } else {
            bpf_map_update_elem(&retransmits, &dst_ip, &retrans, BPF_ANY);
        }
    }

    return TC_ACT_OK;
}

char LICENSE[] SEC("license") = "GPL";

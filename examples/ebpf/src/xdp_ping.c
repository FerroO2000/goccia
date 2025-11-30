// go:build ignore

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/icmp.h>
#include <linux/in.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// Event structure that will be sent to userspace
struct ping_event
{
    __u32 src_ip;
    __u32 dst_ip;
    __u16 id;
    __u16 seq;
};

// Ring buffer map
struct
{
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024); // 256 KB
} ping_events SEC(".maps");

SEC("xdp")
int ping_monitor(struct xdp_md *ctx)
{
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;

    // Parse Ethernet header
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    // Check if it's an IP packet
    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return XDP_PASS;

    // Parse IP header
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;

    // Check if it's ICMP
    if (ip->protocol != IPPROTO_ICMP)
        return XDP_PASS;

    // Parse ICMP header
    struct icmphdr *icmp = (void *)ip + (ip->ihl * 4);
    if ((void *)(icmp + 1) > data_end)
        return XDP_PASS;

    // Check if it's an ICMP echo request (ping)
    if (icmp->type != ICMP_ECHO)
        return XDP_PASS;

    // Reserve space in the ring buffer
    struct ping_event *event;
    event = bpf_ringbuf_reserve(&ping_events, sizeof(*event), 0);
    if (!event)
        return XDP_PASS;

    // Fill the event structure
    event->src_ip = ip->saddr;
    event->dst_ip = ip->daddr;
    event->id = bpf_ntohs(icmp->un.echo.id);
    event->seq = bpf_ntohs(icmp->un.echo.sequence);

    // Submit the event to the ring buffer
    bpf_ringbuf_submit(event, 0);

    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
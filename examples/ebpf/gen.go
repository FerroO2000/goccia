package main

//go:generate go tool bpf2go -tags linux bpf src/xdp_ping.c

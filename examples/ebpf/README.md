## Build Steps

1. Generate the bpf go stubs with `bpf2go`

```bash
go generate
```

2. Build the go program

```bash
go build -o build/
```

3. Run the program as sudoer

```bash
sudo ./build/ebpf myinterface
```
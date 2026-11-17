#!/usr/bin/env python3
"""read_bench.py -- mount-vs-scratch I/O characterisation, no openPMD.

Answers one question before any framework is blamed or credited: what does
this POSIX path actually deliver per stream -- sequentially, and in the
random 4 MiB range reads that dask+BP5 will actually issue?

    python read_bench.py /path/to/file [bytes_to_read]

Works on anything POSIX: Lustre scratch, the EOS mount, a local NVMe.
Prints GiB/s per pattern. One process, O(none) other assumptions.
"""
import os
import sys
import time

SEQ = 32 << 20          # 32 MiB sequential blocks: stream behaviour
RANGE = 4 << 20         # 4 MiB random reads: the dask-chunk shape
DEFAULT_TOTAL = 1 << 30  # 1 GiB per pattern


def gibs(nbytes, secs):
    return (nbytes / 2**30) / secs if secs > 0 else float("inf")


def bench(fd, size, block, total, random_reads, seed=42):
    import random

    rng = random.Random(seed)
    buf = bytearray(block)
    done = 0
    t0 = time.perf_counter()
    while done < total:
        if random_reads:
            # keep ranges page-aligned and inside the file, like BP5 blocks are
            off = rng.randrange(0, max(1, (size - block) >> 12)) << 12
            try:
                os.preadv(fd, [memoryview(buf)], off)
            except OSError as e:  # mounts can refuse vector reads; fall back
                if e.errno != 22:
                    raise
                os.pread(fd, block, off)
        else:
            os.pread(fd, block, (done % max(1, size - block)) & ~0xFFF)
        done += block
        if done > size:
            break
    return gibs(done, time.perf_counter() - t0), done


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    path = sys.argv[1]
    total = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_TOTAL
    size = os.path.getsize(path)
    fd = os.open(path, os.O_RDONLY)
    try:
        t0 = time.perf_counter()
        os.pread(fd, 4096, 0)  # first-touch: open/lookup/auth latency included
        first = (time.perf_counter() - t0) * 1e3
        seq, _ = bench(fd, size, SEQ, total, random_reads=False)
        rnd, n = bench(fd, size, RANGE, total, random_reads=True)
        print(f"{path}")
        print(f"  first pread    {first:8.1f} ms   (open+auth+first byte)")
        print(f"  sequential     {seq:8.2f} GiB/s  ({SEQ >> 20} MiB blocks)")
        print(f"  random 4 MiB   {rnd:8.2f} GiB/s  ({n >> 20} MiB sampled)")
    finally:
        os.close(fd)


if __name__ == "__main__":
    main()

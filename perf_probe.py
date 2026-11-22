# perf_probe.py -- store-regime probe for the Lustre-vs-EOS rate tables.
# modes:  sweep  = read full variables 1..4 (amortization curve: intercept =
#                open/session cost, slope = streaming rate)
#         slabs  = 20 scattered ~128 KiB reads (small-IO regime + cache test:
#                rerun to expose client-side caching: Lustre improves, EOS is
#                bit-repeatably identical)
#         row    = read z-slab K of N (process-scaling curve)
# EOS legs open a staged dir (metadata + s3.json sidecar, see apeos_eos.py);
# stage it on the SHARED filesystem so compute nodes see it.
import sys, time, json, numpy as np, openpmd_api as io
path, mode, tag = sys.argv[1], sys.argv[2], sys.argv[3]
N, K = int(sys.argv[4]), int(sys.argv[5])      # N = concurrent procs (mode=row), K = this proc's index
t0 = time.perf_counter(); s = io.Series(path, io.Access.read_only)
out = {"tag": tag, "mode": mode, "open_s": round(time.perf_counter()-t0, 2)}
rho = s.iterations[0].meshes["rho"]; rc = rho[list(rho)[0]]
gibv = int(np.prod(rc.shape)) * 8 / 2**30
if mode == "sweep":                            # cumulative: 1..4 full variables
    cum, gib, times = 0.0, 0.0, {}
    for v in ("rho", "E", "B", "j"):
        rec = s.iterations[0].meshes[v]; r = rec[list(rec)[0]]
        t = time.perf_counter(); a = r[tuple(slice(0, n) for n in r.shape)]; s.flush()
        dt = time.perf_counter()-t; cum += dt; gib += int(np.prod(r.shape))*8/2**30
        times[v] = (round(dt, 2), round(gib, 2), round(gib/cum, 3))
    out["cumulative_s_GiB_GiBs"] = times
elif mode == "slabs":                          # 20 scattered ~2 MB reads: small-IO regime
    rng = np.random.default_rng(7); dt = []
    for _ in range(20):
        sl = tuple(slice(rng.integers(0, n-k), rng.integers(0, n-k)+k) for n, k in ((512,64),(256,16),(256,16)))
        t = time.perf_counter(); a = rc[sl]; s.flush(); dt.append(time.perf_counter()-t)
    dt = np.array(dt)
    out["slab_s"] = {"min": round(float(dt.min()), 3), "median": round(float(np.median(dt)), 3),
                     "max": round(float(dt.max()), 3), "total": round(float(dt.sum()), 3),
                     "payload_GiB": round(20*64*16*16*8/2**30, 4)}
elif mode == "row":                            # process-scaling: this proc reads 1/N of every var
    z0, z1 = K*512//N, (K+1)*512//N
    t = time.perf_counter()
    a = rc[(slice(z0, z1), slice(None), slice(None))]; s.flush()
    out["share"] = {"k": K, "GiB": round(gibv/N, 3), "s": round(time.perf_counter()-t, 2)}
print(json.dumps(out))

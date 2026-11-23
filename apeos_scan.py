# apeos_scan.py -- per-step evolution scan: the typical plasma-physics
# post-processing pattern = "one pass over MANY steps, one set of scalars
# per step". Runs identically on POSIX scratch (Lustre) and EOS-direct
# (apeos_eos resolver). The scalar curves are simultaneously
#   (a) the physics deliverable (evolution: beam energy, charge, spread,
#       divergence, energy densities, field energy -- vs time/step), and
#   (b) the validation fingerprint: two stores must produce curves that
#       are bit-identical, step by step, quantity by quantity.
#
# Physics per step (particle moments streamed in chunks; fields streamed
# as full-variable passes):
#   Q_nC            beam charge = sum(w) |q|                        [nC]
#   E_mean_MeV      mean total energy <gamma> m c^2                 [MeV]
#   Ekin_mean_MeV   mean kinetic energy                             [MeV]
#   E_rms_MeV       energy spread sqrt(<E^2>-<E>^2), kinetic        [MeV]
#   E_max_MeV       maximum kinetic energy                          [MeV]
#   div_rms_mrad    rms divergence rms(p_perp/p_z) (small angle)   [mrad]
#   u_peak_Jm3      max longitudinal-slab kinetic energy density
#                   (slab = full transverse area x dz_bin)          [J/m^3]
#   u_mean_Jm3      mean kinetic energy density  Ekin_tot/V         [J/m^3]
#   Wfield_J        EM field energy sum(eps0 E^2/2 + B^2/2mu0) dV   [J]
#
# z-binning uses the MESH grid (rho): deterministic, identical bins on
# both stores regardless of chunk/rank partition.
#
# output: <out>/scan.json   one JSON line per step (sorted), plus a
#   final {"kind":"totals","bytes_touched":..,"seconds":..,"ranks":..}
# usage:
#   mpirun -np 32 python3 apeos_scan.py -d ROOT --steps all   -s electrons -o out
#   python3 apeos_scan.py --eos-root EOSROOT --steps 0,50,100 -s electrons -o out
import argparse
import json
import os
import re
import time

import numpy as np
import openpmd_api as io

import apeos_eos
from apeos_particles import OpmdParticles, discover, energy_mev, ME_C2_MEV

E_CH = 1.602176634e-19
MEV_J = 1.602176634e-13
EPS0 = 8.8541878128e-12
MU0 = 1.25663706212e-06

try:
    from mpi4py import MPI
    HAVE_MPI = True
except ImportError:
    HAVE_MPI = False


class SerialComm:
    def Get_size(self):
        return 1

    def Get_rank(self):
        return 0

    def gather(self, obj, root=0):
        return [obj]

    def reduce(self, value, op=None, root=0):
        return value


def unit_geometry(path):
    """(transverse area [m^2], z-origin [m], z-length [m]) from the rho
    mesh; falls back to any mesh. Cached by caller."""
    s = io.Series(path, io.Access.read_only)
    it = s.iterations[list(s.iterations)[-1]]
    ms = it.meshes
    grp = "rho" if "rho" in ms else (list(ms)[0] if len(ms) else None)
    if grp is None:
        s.close()
        return 0.0, 0.0, 0.0
    r = ms[grp][list(ms[grp])[0]]
    rec = ms[grp]      # grid metadata lives on the Record in openPMD >= 0.14
    sp = np.asarray(rec.grid_spacing, dtype=float) * rec.grid_unit_SI
    shp = np.asarray(r.shape, dtype=float)
    pos = np.asarray(rec.position, dtype=float) * rec.grid_unit_SI
    s.close()
    # openPMD 3D cartesian component order (z, x, y) for WarpX diagnostics
    return float(sp[1] * shp[1] * sp[2] * shp[2]), float(pos[0]), float(sp[0] * shp[0])


def step_scalars(reader, a, geom):
    """One streaming particle pass -> weighted moments dict."""
    area, z0, zlen = geom
    W = S = S2 = 0.0
    N = 0
    Emx = 0.0
    Dx = Dy = Dxx = Dyy = 0.0
    SEz = np.zeros(a.zbins)
    Wz = np.zeros(a.zbins)
    for pos, mom, w in reader.stream(a.chunk):
        Ekin = energy_mev(mom[:, 0], mom[:, 1], mom[:, 2]) - ME_C2_MEV
        W += float(np.sum(w))
        S += float(np.sum(Ekin * w))
        S2 += float(np.sum(Ekin * Ekin * w))
        N += int(w.size)
        if w.size:
            Emx = max(Emx, float(Ekin.max()))
            with np.errstate(divide="ignore", invalid="ignore"):
                tx = np.where(mom[:, 2] != 0, mom[:, 0] / mom[:, 2], 0.0)
                ty = np.where(mom[:, 2] != 0, mom[:, 1] / mom[:, 2], 0.0)
            Dx += float(np.sum(tx * w))
            Dy += float(np.sum(ty * w))
            Dxx += float(np.sum(tx * tx * w))
            Dyy += float(np.sum(ty * ty * w))
        if zlen > 0:
            zi = np.clip(((pos[:, 2] - z0) / zlen * a.zbins).astype(int),
                         0, a.zbins - 1)
            np.add.at(SEz, zi, Ekin * w)
            np.add.at(Wz, zi, w)
    out = {"n": N}
    if W > 0:
        mean = S / W
        var = max(S2 / W - mean * mean, 0.0)
        out.update({
            "Q_nC": W * abs(a.charge_e) * E_CH * 1e9,
            "E_mean_MeV": mean + ME_C2_MEV,
            "Ekin_mean_MeV": mean,
            "E_rms_MeV": var ** 0.5,
            "E_max_MeV": Emx,
            "div_rms_mrad": 0.5e3 * ((max(Dxx / W - (Dx / W) ** 2, 0.0)
                                      + max(Dyy / W - (Dy / W) ** 2, 0.0)) ** 0.5),
        })
    if zlen > 0 and area > 0:
        out["u_mean_Jm3"] = (S * MEV_J) / (area * zlen)
        out["u_peak_Jm3"] = (SEz.max() * MEV_J) / (area * zlen / a.zbins)
    return out


def field_energy(path, a):
    """EM field energy [J] + bytes touched (full E,B pass for the step)."""
    s = io.Series(path, io.Access.read_only)
    it = s.iterations[list(s.iterations)[-1]]
    W = 0.0
    nb = 0
    dV = None
    for grp, coef in (("E", 0.5 * EPS0), ("B", 0.5 / MU0)):
        if grp not in it.meshes:
            continue
        for c in ("x", "y", "z"):
            if c not in it.meshes[grp]:
                continue
            rec = it.meshes[grp][c]
            r = rec[list(rec)[0]]
            arr = np.asarray(r[tuple(slice(0, n) for n in r.shape)],
                             dtype=np.float64)
            s.flush()
            nb += arr.nbytes
            if dV is None:
                sp = np.asarray(rec.grid_spacing, dtype=float) * rec.grid_unit_SI
                dV = float(np.prod(sp))
            W += coef * float(np.sum(arr * arr)) * (dV or 0.0)
    s.close()
    return W, nb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--dir", default=None)
    ap.add_argument("-o", "--out", default="apeos_scan")
    ap.add_argument("-s", "--species", default="electrons")
    ap.add_argument("--steps", default="all")
    ap.add_argument("--chunk", type=int, default=1 << 20)
    ap.add_argument("--zbins", type=int, default=512)
    ap.add_argument("--charge-e", type=float, default=-1.0,
                    help="species charge in units of e (electrons: -1)")
    ap.add_argument("--no-fields", action="store_true",
                    help="skip the E,B pass (particle scalars only)")
    apeos_eos.add_arguments(ap)
    a = ap.parse_args()

    if not a.dir and not a.eos_root:
        raise SystemExit("need -d DIR or --eos-root")
    comm = MPI.COMM_WORLD if HAVE_MPI else SerialComm()
    wtime = MPI.Wtime if HAVE_MPI else time.perf_counter
    rank, size = comm.Get_rank(), comm.Get_size()

    eos_leg, local = apeos_eos.resolver(a)
    if eos_leg:
        units = [p for _, p in apeos_eos.units(
            a.eos_root, [int(x) for x in a.steps.split(",") if x.strip()],
            a.eos_host)]
    else:
        if not a.dir:
            raise SystemExit("need -d")
        units = discover(a.dir, None)
        if a.steps != "all":
            pat = re.compile(r"(?:openpmd_|diag_?)(\d{6,})")
            want = {int(x) for x in a.steps.split(",") if x.strip()}
            units = [u for u in units
                     if (m := pat.search(os.path.basename(u)))
                     and int(m.group(1)) in want]
    units.sort()
    if not units and rank == 0:
        raise SystemExit(f"no units for steps={a.steps}")
    mine = units[rank::size]
    if rank == 0:
        os.makedirs(a.out, exist_ok=True)

    t0 = wtime()
    rows, nbytes = [], 0
    geom = None
    for u in mine:
        t = wtime()
        m = re.search(r"(\d{6,})", os.path.basename(u))
        step = int(m.group(1)) if m else -1
        lp = local(u)
        geom = unit_geometry(lp)   # per unit: the domain must never be cached
        reader = OpmdParticles(lp, a.species)
        row = step_scalars(reader, a, geom)
        Wf = 0.0
        if not a.no_fields:
            Wf, nb2 = field_energy(lp, a)
            nbytes += nb2
        nbytes += reader.bytes_read
        row.update({"step": step, "species": a.species, "Wfield_J": Wf,
                    "bytes": nbytes, "seconds": round(wtime() - t, 3)})
        rows.append(row)
        print(f"[scan r{rank}] step {step:6d}"
              f"  Ekin {row.get('Ekin_mean_MeV', 0):10.4f} MeV"
              f"  dE {row.get('E_rms_MeV', 0):8.4f} MeV"
              f"  Q {row.get('Q_nC', 0):8.3f} nC"
              f"  u_pk {row.get('u_peak_Jm3', 0):.3e} J/m3"
              f"  Wf {Wf:.4e} J  [{row['seconds']} s]", flush=True)
    wall = wtime() - t0

    tot_bytes = comm.reduce(int(nbytes), op=(MPI.SUM if HAVE_MPI else None))
    tot_wall = comm.reduce(round(wall, 3), op=(MPI.MAX if HAVE_MPI else None))
    tot = {"bytes_touched": tot_bytes, "seconds": tot_wall, "ranks": size}

    if rank == 0:
        allrows = [r for rr in comm.gather(rows, root=0) for r in rr]
        allrows.sort(key=lambda r: (r["step"], r["species"]))
        with open(os.path.join(a.out, "scan.json"), "w") as fh:
            for r in allrows:
                fh.write(json.dumps(r) + "\n")
            fh.write(json.dumps({"kind": "totals", **tot}) + "\n")
        print(json.dumps({"tool": "scan", "dir": a.dir or a.eos_root,
                          "steps": len(allrows), **tot}))


if __name__ == "__main__":
    main()

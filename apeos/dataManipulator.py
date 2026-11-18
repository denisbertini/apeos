"""Pure-numpy analysis utilities, ported from pp-ana
(gitlab.gsi.de/d.bertini/pp-ana, src/dataManipulator.py, LGPL-3.0-only).

Subset port; extend as analyzers demand more (fft/wigner/FROG live in
the donor until the field analyzer lands)."""

import numpy as np

# electron normalizing constants
ME_C = 9.1093837015e-31 * 299792458.0        # kg m/s
ME_C2_MEV = 0.51099895069                    # MeV


def norma(data):
    """L2 norm (pp-ana port)."""
    return np.sqrt(np.sum(np.abs(data) ** 2))


def width(array, axis=(), factor=0.5, returnIndices=False):
    """FWHM by half-max crossings with nearest-pixel adjustment and
    linear interpolation (exact port of pp-ana dataManipulator.width)."""
    array = np.asarray(array, dtype=float)
    maxInd = int(np.argmax(abs(array)))

    if maxInd != 0:
        indLow = int(np.argmin(abs(array[:maxInd] - array[maxInd] * factor)))
        indUp = int(np.argmin(abs(array[maxInd:] - array[maxInd] * factor)))
        indUp = indUp + maxInd
        dInd = indUp - indLow
    else:
        indLow = indUp = dInd = 0

    if np.size(axis) != 0:
        axis = np.asarray(axis, dtype=float)
        # interpolation between values (searches for the nearest pixel)
        if indLow == 0:
            xLow = axis[1]
        else:
            if abs(array[maxInd] * factor - array[indLow - 1]) > \
               abs(array[maxInd] * factor - array[indLow + 1]):
                indLow = indLow - 1
            m = (array[indLow + 1] - array[indLow]) / (axis[indLow + 1] - axis[indLow])
            b = array[indLow] - m * axis[indLow]
            xLow = (array[maxInd] * factor - b) / m

        if indUp == len(array) - 1:
            xUp = axis[indUp]
        else:
            if abs(array[maxInd] * factor - array[indUp - 1]) < \
               abs(array[maxInd] * factor - array[indUp + 1]):
                indUp = indUp - 1
            m = (array[indUp + 1] - array[indUp]) / (axis[indUp + 1] - axis[indUp])
            b = array[indUp] - m * axis[indUp]
            xUp = (array[maxInd] * factor - b) / m

        if returnIndices:
            return xUp - xLow, indLow, indUp
        return xUp - xLow
    if returnIndices:
        return dInd, indLow, indUp
    return dInd


def moment1d(data, axis, order):
    """1-D moment of the given order (pp-ana port)."""
    if order == 0:
        return np.sum(data)
    return np.sum(data * axis ** order) / np.sum(data)


def energy_mev(px, py, pz):
    """Total electron energy in MeV from SI momentum (kg m/s); the pp-ana
    convention E = m_e c^2 sqrt(1 + p_norm^2)."""
    p2 = (px / ME_C) ** 2 + (py / ME_C) ** 2 + (pz / ME_C) ** 2
    return ME_C2_MEV * np.sqrt(1.0 + p2)

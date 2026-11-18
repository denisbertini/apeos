"""apeos -- post-processing of openPMD archives, in place.

Architecture mirrors pp-ana (2024): a reader class over openPMD
(`apeos.opmd_reader`), a pure-numpy physics library
(`apeos.dataManipulator`), analyzer classes with MPI reductions
(`apeos.particle_analyzer`), and thin argparse drivers. The difference
is the data path: analyzers stream from the openPMD series directly --
scratch or a leos-sealed unit on an EOS mount -- instead of the parquet
intermediate.
"""

from .opmd_reader import opmd_reader          # noqa: F401
from .particle_analyzer import ParticleAnalyzer  # noqa: F401

__version__ = "0.2.0"

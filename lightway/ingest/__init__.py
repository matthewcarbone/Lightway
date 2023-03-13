from pathlib import Path
import sys

p = str(Path(__file__).parent / "_iss_xas_tools")
if p not in sys.path:
    sys.path.append(p)
import xas  # noqa

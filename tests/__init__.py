# type: ignore
import os

import fs

# Add the local code directory to the `fs` module path
fs.__path__.insert(0, os.path.realpath(os.path.join(__file__, "..", "..", "fs")))
fs.opener.__path__.insert(
    0, os.path.realpath(os.path.join(__file__, "..", "..", "fs", "opener"))
)

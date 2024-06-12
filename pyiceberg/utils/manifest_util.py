from functools import lru_cache
from typing import List, Optional

from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestFile, read_manifest_list


@lru_cache
def fetch_manifests(io: FileIO, manifest_list: Optional[str]) -> List[ManifestFile]:
    """Return the manifests for the given snapshot."""
    if manifest_list not in (None, ""):
        file = io.new_input(manifest_list)  # type: ignore
        return list(read_manifest_list(file))
    return []

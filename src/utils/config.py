"""Load pipeline parameters from params.yaml."""

from pathlib import Path

import yaml

PARAMS_PATH = Path("params.yaml")


def load_params(path: Path = PARAMS_PATH) -> dict:
    """Return params.yaml as a dict.

    Raises:
        FileNotFoundError: If the params file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"Params file not found: {path}")
    with path.open() as f:
        return yaml.safe_load(f)

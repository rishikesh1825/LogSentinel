"""Single source of truth for the drain3 configuration.

Template mining, Spark-side matching and the served MLflow model all build
drain3 through ``build_config`` so that masking and tree settings are identical
at training and inference time.
"""

from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig

# Masks are applied in order, so specific patterns come before generic ones.
MASKS = [
    # A run of 2+ block IDs (e.g. "ask X to delete blk_1 blk_2 ...") becomes one token,
    # so the template does not depend on how many blocks are listed.
    (r"blk_-?\d+(?:\s+blk_-?\d+)+", "BLKS"),
    (r"blk_-?\d+", "BLK"),
    (r"(\d{1,3}\.){3}\d{1,3}(:\d+)?", "IP"),
    # File paths with 2+ segments (job files, block files). Masked before numbers so the
    # digits inside a path do not break it up. "/<IP>" is not matched ("<" is excluded).
    (r"(?:/[\w.\-]+){2,}", "PATH"),
    (r"((?<=[^A-Za-z0-9])|^)(-?\d+)((?=[^A-Za-z0-9])|$)", "NUM"),
]


def build_config(sim_th: float, depth: int, max_children: int) -> TemplateMinerConfig:
    """Return a drain3 config with LogSentinel's masking rules and tree settings."""
    config = TemplateMinerConfig()
    config.masking_instructions = [MaskingInstruction(p, name) for p, name in MASKS]
    config.mask_prefix, config.mask_suffix = "<", ">"
    config.drain_sim_th = sim_th
    config.drain_depth = depth
    config.drain_max_children = max_children
    config.profiling_enabled = False
    return config

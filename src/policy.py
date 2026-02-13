import os
from dataclasses import dataclass


@dataclass
class SourcePolicy:
    retailer: str
    allowed_modes: list[str]
    scrape_mode: str
    scrape_mode_message: str

    def is_allowed(self, mode: str) -> bool:
        return mode in self.allowed_modes


def _fallback_policy() -> SourcePolicy:
    return SourcePolicy(
        retailer="homedepot_ca",
        allowed_modes=["partner_api"],
        scrape_mode="disabled",
        scrape_mode_message="HomeDepot.ca scraping is disabled by policy (CGU). Use partner_api/feed/permissioned source.",
    )


def load_homedepot_policy(path: str = "policy/sources.yml") -> SourcePolicy:
    if not os.path.exists(path):
        return _fallback_policy()

    allowed_modes: list[str] = []
    scrape_mode = "disabled"
    scrape_msg = _fallback_policy().scrape_mode_message

    with open(path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]

    in_homedepot = False
    in_allowed_modes = False
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if raw.startswith("homedepot_ca:"):
            in_homedepot = True
            continue
        if not in_homedepot:
            continue
        if raw.startswith("  allowed_modes:"):
            in_allowed_modes = True
            continue
        if in_allowed_modes and raw.startswith("    -"):
            allowed_modes.append(line.replace("-", "", 1).strip().strip('"'))
            continue
        in_allowed_modes = False
        if raw.startswith("  scrape_mode:"):
            scrape_mode = line.split(":", 1)[1].strip().strip('"')
        if raw.startswith("  scrape_mode_message:"):
            scrape_msg = line.split(":", 1)[1].strip().strip('"')

    return SourcePolicy(
        retailer="homedepot_ca",
        allowed_modes=allowed_modes or ["partner_api"],
        scrape_mode=scrape_mode,
        scrape_mode_message=scrape_msg,
    )

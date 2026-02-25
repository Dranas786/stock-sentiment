from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable
import requests

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "shared" / "symbols.txt"


def _download_text(url: str) -> str:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def _parse_pipe_file(text: str) -> list[dict[str, str]]:
    """
    Nasdaq Trader symbol files are pipe-delimited with a header line.
    Last line is usually a footer like: "File Creation Time: ..."
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []

    header = lines[0].split("|")
    rows: list[dict[str, str]] = []

    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            break
        parts = ln.split("|")
        if len(parts) != len(header):
            continue
        rows.append(dict(zip(header, parts)))

    return rows


def _clean_symbol(sym: str) -> str:
    return sym.strip().upper()


def build_symbols(*, include_etfs: bool = False) -> list[str]:
    nasdaq_rows = _parse_pipe_file(_download_text(NASDAQ_LISTED_URL))
    other_rows = _parse_pipe_file(_download_text(OTHER_LISTED_URL))

    symbols: set[str] = set()

    # nasdaqlisted.txt columns include: Symbol, Test Issue, ETF, ...
    for row in nasdaq_rows:
        sym = _clean_symbol(row.get("Symbol", ""))
        if not sym:
            continue
        if row.get("Test Issue", "N") != "N":
            continue
        if not include_etfs and row.get("ETF", "N") == "Y":
            continue
        symbols.add(sym)

    # otherlisted.txt columns include: ACT Symbol, Test Issue, ETF, ...
    for row in other_rows:
        sym = _clean_symbol(row.get("ACT Symbol", ""))
        if not sym:
            continue
        if row.get("Test Issue", "N") != "N":
            continue
        if not include_etfs and row.get("ETF", "N") == "Y":
            continue
        symbols.add(sym)

    # Optional: drop symbols with characters your extractor doesn’t support yet.
    # Right now you said regex matches 1–5 uppercase letters; so keep those only.
    # You can loosen this later (e.g., allow '.' for BRK.B).
    symbols = {s for s in symbols if re.fullmatch(r"[A-Z]{1,5}", s)}

    return sorted(symbols)


def write_symbols_file(symbols: Iterable[str]) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(symbols) + "\n", encoding="utf-8")


if __name__ == "__main__":
    syms = build_symbols(include_etfs=False)
    write_symbols_file(syms)
    print(f"Wrote {len(syms)} symbols to {OUT}")
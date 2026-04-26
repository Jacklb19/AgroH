"""
Reemplaza caracteres Unicode no-ASCII en mensajes de logger.info/warning
de todos los archivos .py del proyecto para evitar UnicodeEncodeError en
consolas Windows (cp1252).
"""
import re
from pathlib import Path

REPLACEMENTS = {
    "→": "->",
    "\u2192": "->",
    "≤": "<=",
    "\u2264": "<=",
    "≥": ">=",
    "\u2265": ">=",
}

roots = ["extract", "load", "clean", "validate"]

for root in roots:
    for fpath in Path(root).rglob("*.py"):
        text = fpath.read_text(encoding="utf-8")
        new_text = text
        for old, new in REPLACEMENTS.items():
            new_text = new_text.replace(old, new)
        if new_text != text:
            fpath.write_text(new_text, encoding="utf-8")
            print(f"Fixed: {fpath}")

print("Done")

#!/usr/bin/env python3
"""Update Python support config from official Python release status."""

from __future__ import annotations

import datetime as dt
import html
import re
import sys
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "pyproject.toml"
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
MAKEFILE = ROOT / "Makefile"

DEVGUIDE_VERSIONS_URL = "https://devguide.python.org/versions/"


def fetch(url: str) -> str:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def _strip_tags(raw_html: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", raw_html)
    return html.unescape(no_tags).strip()


def _parse_date(value: str) -> dt.date | None:
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def released_supported_python_versions(page_html: str) -> list[str]:
    table_match = re.search(
        r'<section id="supported-versions">.*?<table[^>]*>(.*?)</table>',
        page_html,
        flags=re.DOTALL,
    )
    if not table_match:
        raise RuntimeError(
            "Could not find supported versions table on Python devguide page."
        )

    table_html = table_match.group(1)
    row_html_list = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.DOTALL)
    today = dt.date.today()
    versions: list[str] = []

    for row_html in row_html_list:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.DOTALL)
        if len(cells) < 4:
            continue

        branch = _strip_tags(cells[0])
        first_release_raw = _strip_tags(cells[3])

        if not re.fullmatch(r"3\.\d+", branch):
            continue

        first_release = _parse_date(first_release_raw)
        if first_release is None:
            continue

        # Include only already released versions.
        if first_release <= today:
            versions.append(branch)

    if not versions:
        raise RuntimeError(
            "No released Python 3.x versions found from supported versions table."
        )

    versions.sort(key=lambda x: tuple(int(part) for part in x.split(".")))
    return versions


def compute_python_constraint(versions: list[str]) -> str:
    min_version = versions[0]
    latest_major, latest_minor = map(int, versions[-1].split("."))
    return f">={min_version},<{latest_major}.{latest_minor + 1}"


def replace_or_fail(
    content: str, pattern: str, replacement: str, file_label: str
) -> str:
    new_content, count = re.subn(
        pattern, replacement, content, count=1, flags=re.MULTILINE
    )
    if count != 1:
        raise RuntimeError(
            f"Failed updating {file_label}: pattern not found: {pattern}"
        )
    return new_content


def update_file(path: Path, updater) -> bool:
    original = path.read_text()
    updated = updater(original)
    if updated != original:
        path.write_text(updated)
        return True
    return False


def main() -> int:
    page_html = fetch(DEVGUIDE_VERSIONS_URL)
    versions = released_supported_python_versions(page_html)

    version_list_literal = ", ".join(f'"{v}"' for v in versions)
    version_list_space = " ".join(versions)
    python_constraint = compute_python_constraint(versions)

    changed: list[str] = []

    if update_file(
        PYPROJECT,
        lambda content: replace_or_fail(
            content,
            r'^python\s*=\s*".*"$',
            f'python = "{python_constraint}"',
            "pyproject.toml python constraint",
        ),
    ):
        changed.append(str(PYPROJECT.relative_to(ROOT)))

    if update_file(
        CI_WORKFLOW,
        lambda content: replace_or_fail(
            content,
            r"python-version:\s*\[[^\]]*\]",
            f"python-version: [{version_list_literal}]",
            "ci.yml python matrix",
        ),
    ):
        changed.append(str(CI_WORKFLOW.relative_to(ROOT)))

    if update_file(
        MAKEFILE,
        lambda content: replace_or_fail(
            content,
            r"^PYTHON_VERSIONS \?= .*$",
            f"PYTHON_VERSIONS ?= {version_list_space}",
            "Makefile PYTHON_VERSIONS",
        ),
    ):
        changed.append(str(MAKEFILE.relative_to(ROOT)))

    if changed:
        print("Updated files:")
        for file_name in changed:
            print(f" - {file_name}")
    else:
        print("No file changes needed.")

    print(f"Supported released versions: {', '.join(versions)}")
    print(f"Python constraint: {python_constraint}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

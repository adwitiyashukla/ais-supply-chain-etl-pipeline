from __future__ import annotations

import ast
import io
import subprocess
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {"data", ".git", ".venv", "venv", "__pycache__"}


def tracked_python_files():
    try:
        output = subprocess.run(
            ["git", "ls-files", "*.py"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.split()
        files = [ROOT / name for name in output]
    except (subprocess.CalledProcessError, FileNotFoundError):
        files = []
    if not files:
        files = [
            item
            for item in ROOT.rglob("*.py")
            if not SKIP.intersection(item.relative_to(ROOT).parts)
        ]
    return [item for item in files if item.exists()]


def comment_lines(path):
    source = path.read_text()
    found = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            found.append((path.name, token.start[0], token.string))
    return found


def docstrings(path):
    tree = ast.parse(path.read_text())
    found = []
    for node in ast.walk(tree):
        holder = isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        if holder and ast.get_docstring(node) is not None:
            found.append((path.name, getattr(node, "name", "module")))
    return found


def test_source_files_exist():
    assert len(tracked_python_files()) >= 20


def test_no_comments_anywhere():
    offenders = []
    for path in tracked_python_files():
        offenders.extend(comment_lines(path))
    allowed = [item for item in offenders if item[2].startswith("# noqa")]
    assert [item for item in offenders if item not in allowed] == []


def test_no_docstrings_anywhere():
    offenders = []
    for path in tracked_python_files():
        offenders.extend(docstrings(path))
    assert offenders == []

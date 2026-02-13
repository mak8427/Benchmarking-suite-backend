"""Validate function docstrings for args and multi-line descriptions."""

from __future__ import annotations

import argparse
import ast
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DocstringIssue:
    """A docstring validation issue.

    Examples:
        >>> issue = DocstringIssue(Path("demo.py"), 10, "missing docstring")
        >>> "demo.py" in str(issue)
        True
    """

    path: Path
    line: int
    message: str

    def __str__(self) -> str:
        """Return a formatted issue string.

        Returns:
            str: Formatted message.

        Examples:
            >>> str(DocstringIssue(Path('a.py'), 1, 'x'))
            'a.py:1: x'
        """
        return f"{self.path}:{self.line}: {self.message}"


def _main() -> int:
    """Run the docstring validation script.

    Returns:
        int: Exit code.

    Examples:
        >>> isinstance(_main.__name__, str)
        True
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()

    issues: list[DocstringIssue] = []
    for path in _iter_python_files(args.root):
        issues.extend(_check_file(path))

    for issue in issues:
        print(issue)
    return 1 if issues else 0


def _iter_python_files(root: Path) -> list[Path]:
    """Collect Python files under the given root.

    Args:
        root (Path): Root directory to scan.

    Returns:
        list[Path]: Python files to analyze.

    Examples:
        >>> any(p.name == 'main.py' for p in _iter_python_files(Path('.')))
        True
    """
    files: list[Path] = []
    for path in root.rglob("*.py"):
        if _should_skip(path):
            continue
        files.append(path)
    return files


def _should_skip(path: Path) -> bool:
    """Return whether a path should be skipped.

    Args:
        path (Path): Candidate path.

    Returns:
        bool: ``True`` if the path is excluded.

    Examples:
        >>> _should_skip(Path('__pycache__/x.py'))
        True
        >>> _should_skip(Path('api_backend/main.py'))
        False
    """
    parts = set(path.parts)
    return any(
        name in parts
        for name in (
            ".git",
            ".venv",
            "__pycache__",
            "build",
            "dist",
        )
    )


def _check_file(path: Path) -> list[DocstringIssue]:
    """Check all functions in a file.

    Args:
        path (Path): Python file to inspect.

    Returns:
        list[DocstringIssue]: Detected docstring issues.

    Examples:
        >>> isinstance(_check_file(Path('scripts/check_docstrings.py')), list)
        True
    """
    issues: list[DocstringIssue] = []
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            issues.extend(_check_function(path, node))
    return issues


def _check_function(
    path: Path,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> list[DocstringIssue]:
    """Validate a single function docstring.

    Args:
        path (Path): File containing the function.
        node (ast.AST): Function AST node.

    Returns:
        list[DocstringIssue]: Detected issues.

    Examples:
        >>> src = "def f(x):\\n    \\\"\\\"\\\"Desc.\\n\\nArgs:\\n    x: value.\\n    \\\"\\\"\\\"\\n    return x\\n"
        >>> module = ast.parse(src)
        >>> fn = module.body[0]
        >>> _check_function(Path('x.py'), fn)
        []
    """
    issues: list[DocstringIssue] = []
    docstring = ast.get_docstring(node)
    if not docstring:
        return [DocstringIssue(path, node.lineno, "missing docstring")]
    if _count_non_empty_lines(docstring) < 2:
        issues.append(
            DocstringIssue(
                path,
                node.lineno,
                "docstring must include a multi-line description",
            )
        )

    params = _function_params(node)
    if params:
        issues.extend(_check_args_section(path, node, docstring, params))
    return issues


def _count_non_empty_lines(docstring: str) -> int:
    """Count non-empty lines in a docstring.

    Args:
        docstring (str): Docstring content.

    Returns:
        int: Number of non-empty lines.

    Examples:
        >>> _count_non_empty_lines('a\\n\\n b')
        2
    """
    return sum(1 for line in docstring.splitlines() if line.strip())


def _function_params(node: ast.AST) -> list[str]:
    """Return function parameter names excluding self/cls.

    Args:
        node (ast.AST): Function AST node.

    Returns:
        list[str]: Parameter names.

    Examples:
        >>> mod = ast.parse('def f(self, x, *, y):\\n    pass\\n')
        >>> _function_params(mod.body[0])
        ['x', 'y']
    """
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return []
    params = []
    args = node.args
    for arg in args.posonlyargs + args.args + args.kwonlyargs:
        if arg.arg in {"self", "cls"}:
            continue
        params.append(arg.arg)
    if args.vararg:
        params.append(args.vararg.arg)
    if args.kwarg:
        params.append(args.kwarg.arg)
    return params


def _check_args_section(
    path: Path,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    docstring: str,
    params: list[str],
) -> list[DocstringIssue]:
    """Validate Args section contents.

    Args:
        path (Path): File containing the function.
        node (ast.AST): Function AST node.
        docstring (str): Function docstring.
        params (list[str]): Parameter names to validate.

    Returns:
        list[DocstringIssue]: Detected issues.

    Examples:
        >>> _check_args_section(Path('x.py'), ast.parse('def f(a):\\n    pass').body[0], 'x', ['a'])[0].message
        'docstring must include an Args section'
    """
    issues: list[DocstringIssue] = []
    lines = docstring.splitlines()
    args_index = _find_args_line(lines)
    if args_index is None:
        return [
            DocstringIssue(
                path,
                node.lineno,
                "docstring must include an Args section",
            )
        ]
    documented = _parse_args_section(lines[args_index + 1 :])
    missing = [param for param in params if param not in documented]
    if missing:
        issues.append(
            DocstringIssue(
                path,
                node.lineno,
                f"Args section missing: {', '.join(missing)}",
            )
        )
    return issues


def _find_args_line(lines: list[str]) -> int | None:
    """Locate the Args section line index.

    Args:
        lines (list[str]): Docstring lines.

    Returns:
        int | None: Args line index or ``None``.

    Examples:
        >>> _find_args_line(['Title', 'Args:', ' x: y'])
        1
    """
    for index, line in enumerate(lines):
        if line.strip() == "Args:":
            return index
    return None


def _parse_args_section(lines: list[str]) -> set[str]:
    """Parse documented argument names from an Args section.

    Args:
        lines (list[str]): Lines following the Args header.

    Returns:
        set[str]: Documented parameter names.

    Examples:
        >>> sorted(_parse_args_section(['    a: x', '    b (int): y']))
        ['a', 'b']
    """
    documented: set[str] = set()
    for line in lines:
        if not line.strip():
            continue
        if _is_section_header(line):
            break
        match = re.match(r"^\s*([*]{0,2}[A-Za-z_][A-Za-z0-9_]*)\s*(\(|:)", line)
        if not match:
            continue
        name = match.group(1).lstrip("*")
        documented.add(name)
    return documented


def _is_section_header(line: str) -> bool:
    """Return whether a line looks like a section header.

    Args:
        line (str): Line content.

    Returns:
        bool: ``True`` when line matches a section heading pattern.

    Examples:
        >>> _is_section_header('Returns:')
        True
        >>> _is_section_header('    x: value')
        False
    """
    stripped = line.strip()
    return stripped.endswith(":") and stripped != "Args:"


if __name__ == "__main__":
    raise SystemExit(_main())

"""Generate silver_er.png from silver_er.mmd using Graphviz.

From the repository root: python3 docs/render_silver_er.py
Mermaid is the editable diagram source. No SVG or DOT files are saved.
"""

from __future__ import annotations

import html
import re
import shutil
import subprocess
from pathlib import Path

DIAGRAMS = Path(__file__).resolve().parent / "diagrams"


def read_mermaid(source: str) -> dict:
    """Read this diagram's table attributes and crow's-foot relationships."""
    tables = {}
    for table, body in re.findall(
        r"^    (\w+) \{\n(.*?)^    \}", source, re.MULTILINE | re.DOTALL
    ):
        columns = []
        for line in body.splitlines():
            match = re.fullmatch(
                r"\s*(BIGINT|INT|STRING|DATE|DECIMAL_18_2) (\w+)"
                r'(?: (PK, FK|PK|FK))? "([^"]+)"',
                line,
            )
            if match is None:
                raise ValueError(f"Unrecognized attribute in {table}: {line}")
            dtype, name, keys, comment = match.groups()
            columns.append(
                {
                    "name": name,
                    "type": "DECIMAL(18, 2)" if dtype == "DECIMAL_18_2" else dtype,
                    "nullable": comment.startswith("NULLABLE"),
                    "keys": keys.split(", ") if keys else [],
                }
            )
        tables[table] = {
            "columns": columns,
            "primary_key": [c["name"] for c in columns if "PK" in c["keys"]],
        }
    if len(tables) != 8 or sum(len(t["columns"]) for t in tables.values()) != 61:
        raise ValueError("Expected the eight TPC-H tables and all 61 attributes.")
    foreign_keys = []
    for parent, connector, cardinality, child, label in re.findall(
        r'^    (\w+) \|\|(--|\.\.)(o\{|\|\{) (\w+) : "([^"]+)"$',
        source,
        re.MULTILINE,
    ):
        columns = label.split(", ")
        parent_types = {c["name"]: c["type"] for c in tables[parent]["columns"]}
        child_types = {c["name"]: c["type"] for c in tables[child]["columns"]}
        if columns != tables[parent]["primary_key"]:
            raise ValueError(f"Foreign key must reference the whole PK of {parent}.")
        if any(parent_types[c] != child_types[c] for c in columns):
            raise ValueError(f"Foreign key type mismatch: {parent} -> {child}.")
        foreign_keys.append(
            {
                "table": child,
                "references_table": parent,
                "columns": columns,
                "minimum_children": cardinality == "|{",
                "redundant": connector == "..",
            }
        )
    if len(foreign_keys) != 10:
        raise ValueError("Expected all ten documented foreign-key relationships.")
    return {"tables": tables, "foreign_keys": foreign_keys}


def graphviz_source(model: dict) -> str:
    """Use exact SQL types and readable table rows in the presentation image."""
    lines = [
        "// Generated from silver_er.mmd.",
        "digraph SilverER {",
        'graph [rankdir=LR, bgcolor="#f8fafc", pad="0.35", nodesep="0.65",',
        'ranksep="1.0", splines=polyline, dpi=180, fontname="Helvetica",',
        'labelloc=t, label="TPC-H Logistics | Silver schema\\nworkspace.tpch_silver | 8 tables | 61 columns", fontsize=26];',
        'node [shape=plain, fontname="Helvetica"];',
        'edge [fontname="Helvetica", fontsize=10, color="#64748b",',
        'fontcolor="#334155", dir=both, arrowtail=tee, arrowsize=0.8];',
    ]
    for table, spec in model["tables"].items():
        color = "#0f766e" if table in ("orders", "lineitem") else "#1e3a8a"
        rows = [
            (
                f'<TR><TD COLSPAN="3" BGCOLOR="{color}" CELLPADDING="10">'
                f'<FONT COLOR="white" POINT-SIZE="18"><B>{table}</B></FONT></TD></TR>'
            ),
            (
                '<TR><TD ALIGN="LEFT"><B>Column</B></TD>'
                '<TD ALIGN="LEFT"><B>SQL type</B></TD><TD><B>Key</B></TD></TR>'
            ),
        ]
        for index, col in enumerate(spec["columns"]):
            keys = col["keys"]
            name = html.escape(col["name"]) + (" ?" if col["nullable"] else "")
            bgcolor = "#eff6ff" if keys else ("white" if index % 2 else "#f1f5f9")
            rows.append(
                f'<TR><TD PORT="{col["name"]}" ALIGN="LEFT" BGCOLOR="{bgcolor}">'
                f'{name}</TD><TD ALIGN="LEFT" BGCOLOR="{bgcolor}">'
                f'{html.escape(col["type"])}</TD><TD BGCOLOR="{bgcolor}">'
                f"{', '.join(keys)}</TD></TR>"
            )
        pk = html.escape(", ".join(spec["primary_key"]))
        rows.append(
            '<TR><TD COLSPAN="3" ALIGN="LEFT" BGCOLOR="#e2e8f0">'
            f'<FONT POINT-SIZE="10">PK ({pk})</FONT></TD></TR>'
        )
        table_html = (
            '<TABLE BORDER="1" COLOR="#cbd5e1" CELLBORDER="0" '
            'CELLSPACING="0" CELLPADDING="6">' + "".join(rows) + "</TABLE>"
        )
        lines.append(f"{table} [label=<{table_html}>];")
    for fk in model["foreign_keys"]:
        parent = fk["references_table"]
        child = fk["table"]
        label = ", ".join(fk["columns"])
        arrow = "crowtee" if fk["minimum_children"] else "crowodot"
        style = ", style=dashed, constraint=false" if fk["redundant"] else ""
        lines.append(
            f'{parent} -> {child} [label="{label}", arrowhead="{arrow}"{style}];'
        )
    lines.extend(
        [
            'legend [label=<<TABLE BORDER="0" CELLBORDER="0" CELLPADDING="6">',
            '<TR><TD ALIGN="LEFT"><B>Reading the diagram</B></TD></TR>',
            '<TR><TD ALIGN="LEFT">PK / FK: primary / foreign key</TD></TR>',
            '<TR><TD ALIGN="LEFT">? = nullable; other columns are required</TD></TR>',
            '<TR><TD ALIGN="LEFT">Composite PK: use all columns in the footer</TD></TR>',
            '<TR><TD ALIGN="LEFT">partsupp to lineitem: composite FK (part_key, supp_key)</TD></TR>',
            '<TR><TD ALIGN="LEFT">Parent: exactly one | Child: zero or many</TD></TR>',
            '<TR><TD ALIGN="LEFT">Exception: every order has one or more line items</TD></TR>',
            '<TR><TD ALIGN="LEFT">Dashed links: implied direct part/supplier references</TD></TR>',
            '<TR><TD ALIGN="LEFT">Intended validated schema; Silver ETL enforces keys</TD></TR>',
            "</TABLE>>];",
            "}",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    executable = shutil.which("dot")
    if executable is None:
        raise SystemExit("Graphviz is required to regenerate the PNG.")
    model = read_mermaid((DIAGRAMS / "silver_er.mmd").read_text())
    subprocess.run(
        [executable, "-Tpng", "-o", str(DIAGRAMS / "silver_er.png")],
        input=graphviz_source(model),
        text=True,
        check=True,
    )
    print("Validated Mermaid and generated silver_er.png (8 tables, 61 columns).")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable

from benchlib.scenarios import reset_dir


DEFAULT_SCENARIOS: tuple[dict[str, Any], ...] = (
    {"name": "scene1_linear_100x10", "num_dags": 100, "num_tasks": 10, "topology": "linear"},
    {"name": "scene2_linear_10x100", "num_dags": 10, "num_tasks": 100, "topology": "linear"},
    {"name": "scene3_tree_100x10", "num_dags": 100, "num_tasks": 10, "topology": "tree"},
    {"name": "scene7_diamond_100x10", "num_dags": 100, "num_tasks": 10, "topology": "diamond"},
    {"name": "scene8_fanout_100x10", "num_dags": 100, "num_tasks": 10, "topology": "fanout"},
    {"name": "scene9_fanin_100x10", "num_dags": 100, "num_tasks": 10, "topology": "fanin"},
    {"name": "scene10_mesh_100x10", "num_dags": 100, "num_tasks": 10, "topology": "mesh"},
    {"name": "scene4_burst_ready_1x100", "num_dags": 1, "num_tasks": 0, "topology": "burst_ready", "burst_width": 100},
    {"name": "scene5_burst_ready_1x500", "num_dags": 1, "num_tasks": 0, "topology": "burst_ready", "burst_width": 500},
    {"name": "scene6_burst_ready_1x1000", "num_dags": 1, "num_tasks": 0, "topology": "burst_ready", "burst_width": 1000},
)


def emit_task_lines(task_id: str, deps: list[str] | None = None) -> list[str]:
    lines = [
        "[[tasks]]",
        f'id = "{task_id}"',
        'executor = "noop"',
    ]
    if deps:
        deps_list = ", ".join(f'"{dep}"' for dep in deps)
        lines.append(f"dependencies = [{deps_list}]")
    lines.append("")
    return lines


def build_dag_text(
    dag_id: str,
    num_tasks: int,
    topology: str = "linear",
    burst_width: int | None = None,
) -> str:
    lines = [
        f'id = "{dag_id}"',
        f'name = "Airflow Bench {dag_id}"',
        'description = "Benchmark DAG"',
        "max_concurrent_runs = 1000",
        "",
    ]

    if topology == "burst_ready":
        if burst_width is None:
            raise ValueError("burst_ready topology requires burst_width")
        for i in range(burst_width + 1):
            deps = ["t0"] if i > 0 else None
            lines.extend(emit_task_lines(f"t{i}", deps))
        return "\n".join(lines).rstrip() + "\n"

    if topology == "diamond":
        if num_tasks < 3:
            raise ValueError("diamond topology requires at least 3 tasks")
        lines.extend(emit_task_lines("t0"))
        for i in range(1, num_tasks - 1):
            lines.extend(emit_task_lines(f"t{i}", ["t0"]))
        lines.extend(
            emit_task_lines(
                f"t{num_tasks - 1}",
                [f"t{i}" for i in range(1, num_tasks - 1)],
            )
        )
        return "\n".join(lines).rstrip() + "\n"

    if topology == "fanout":
        if num_tasks < 2:
            raise ValueError("fanout topology requires at least 2 tasks")
        lines.extend(emit_task_lines("t0"))
        for i in range(1, num_tasks):
            lines.extend(emit_task_lines(f"t{i}", ["t0"]))
        return "\n".join(lines).rstrip() + "\n"

    if topology == "fanin":
        if num_tasks < 2:
            raise ValueError("fanin topology requires at least 2 tasks")
        for i in range(num_tasks - 1):
            lines.extend(emit_task_lines(f"t{i}"))
        lines.extend(
            emit_task_lines(
                f"t{num_tasks - 1}",
                [f"t{i}" for i in range(num_tasks - 1)],
            )
        )
        return "\n".join(lines).rstrip() + "\n"

    if topology == "mesh":
        if num_tasks < 4:
            raise ValueError("mesh topology requires at least 4 tasks")
        front = max(1, num_tasks // 3)
        middle = max(1, num_tasks // 3)
        back = num_tasks - front - middle
        if back <= 0:
            back = 1
            if front > 1:
                front -= 1
            else:
                middle -= 1
        layers = [
            list(range(0, front)),
            list(range(front, front + middle)),
            list(range(front + middle, num_tasks)),
        ]
        for idx, layer in enumerate(layers):
            prev_layer = layers[idx - 1] if idx > 0 else []
            for task_idx in layer:
                deps = [f"t{dep}" for dep in prev_layer]
                lines.extend(emit_task_lines(f"t{task_idx}", deps or None))
        return "\n".join(lines).rstrip() + "\n"

    for i in range(num_tasks):
        deps = None
        if topology == "linear" and i > 0:
            deps = [f"t{i - 1}"]
        elif topology == "tree" and i > 0:
            deps = [f"t{(i - 1) // 2}"]
        lines.extend(emit_task_lines(f"t{i}", deps))
    return "\n".join(lines).rstrip() + "\n"


def write_toml(
    path: str | Path,
    dag_id: str,
    num_tasks: int,
    topology: str = "linear",
    burst_width: int | None = None,
) -> None:
    target = Path(path)
    target.write_text(
        build_dag_text(dag_id, num_tasks, topology=topology, burst_width=burst_width),
        encoding="utf-8",
    )


def setup_scenario(base_dir: str | Path, spec: dict[str, Any]) -> None:
    scenario_dir = Path(base_dir) / str(spec["name"])
    reset_dir(scenario_dir)

    for i in range(int(spec["num_dags"])):
        dag_id = f"{spec['name']}_dag_{i}"
        write_toml(
            scenario_dir / f"{dag_id}.toml",
            dag_id,
            int(spec["num_tasks"]),
            str(spec["topology"]),
            burst_width=spec.get("burst_width"),
        )

    if spec["topology"] == "burst_ready":
        print(
            f"Generated {spec['name']}: {spec['num_dags']} DAGs, "
            f"burst_width={spec.get('burst_width')} "
            f"(total_tasks_per_dag={(spec.get('burst_width') or 0) + 1})"
        )
    else:
        print(
            f"Generated {spec['name']}: {spec['num_dags']} DAGs, "
            f"{spec['num_tasks']} tasks/DAG ({spec['topology']})"
        )


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic Airflow benchmark DAGs")
    parser.add_argument(
        "--base-dir",
        default="bench/airflow_dags",
        help="Output root for generated DAG scenario directories",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    base_dir = Path(args.base_dir)
    for spec in DEFAULT_SCENARIOS:
        setup_scenario(base_dir, spec)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

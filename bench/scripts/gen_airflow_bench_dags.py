import os
import shutil


def write_toml(path, dag_id, num_tasks, topology="linear", burst_width=None):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f'id = "{dag_id}"\n')
        f.write(f'name = "Airflow Bench {dag_id}"\n')
        f.write('description = "Benchmark DAG"\n')
        f.write("max_concurrent_runs = 1000\n\n")

        def emit_task(task_id, deps=None):
            f.write("[[tasks]]\n")
            f.write(f'id = "{task_id}"\n')
            f.write('executor = "noop"\n')
            if deps:
                deps_list = ", ".join(f'"{dep}"' for dep in deps)
                f.write(f"dependencies = [{deps_list}]\n")
            f.write("\n")

        if topology == "burst_ready":
            if burst_width is None:
                raise ValueError("burst_ready topology requires burst_width")
            total_tasks = burst_width + 1  # root + one burst layer
            for i in range(total_tasks):
                deps = ["t0"] if i > 0 else None
                emit_task(f"t{i}", deps)
            return

        if topology == "diamond":
            if num_tasks < 3:
                raise ValueError("diamond topology requires at least 3 tasks")
            emit_task("t0")
            for i in range(1, num_tasks - 1):
                emit_task(f"t{i}", ["t0"])
            emit_task(
                f"t{num_tasks - 1}",
                [f"t{i}" for i in range(1, num_tasks - 1)],
            )
            return

        if topology == "fanout":
            if num_tasks < 2:
                raise ValueError("fanout topology requires at least 2 tasks")
            emit_task("t0")
            for i in range(1, num_tasks):
                emit_task(f"t{i}", ["t0"])
            return

        if topology == "fanin":
            if num_tasks < 2:
                raise ValueError("fanin topology requires at least 2 tasks")
            for i in range(num_tasks - 1):
                emit_task(f"t{i}")
            emit_task(f"t{num_tasks - 1}", [f"t{i}" for i in range(num_tasks - 1)])
            return

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
                    emit_task(f"t{task_idx}", deps or None)
            return

        for i in range(num_tasks):
            deps = None
            if topology == "linear" and i > 0:
                deps = [f"t{i-1}"]
            elif topology == "tree" and i > 0:
                deps = [f"t{(i - 1) // 2}"]
            emit_task(f"t{i}", deps)


def setup_scenario(base_dir, scenario_name, num_dags, num_tasks, topology, burst_width=None):
    scenario_dir = os.path.join(base_dir, scenario_name)
    if os.path.exists(scenario_dir):
        shutil.rmtree(scenario_dir)
    os.makedirs(scenario_dir)

    for i in range(num_dags):
        dag_id = f"{scenario_name}_dag_{i}"
        file_path = os.path.join(scenario_dir, f"{dag_id}.toml")
        write_toml(
            file_path,
            dag_id,
            num_tasks,
            topology,
            burst_width=burst_width,
        )

    if topology == "burst_ready":
        print(
            f"Generated {scenario_name}: {num_dags} DAGs, burst_width={burst_width} "
            f"(total_tasks_per_dag={burst_width + 1})"
        )
    else:
        print(
            f"Generated {scenario_name}: {num_dags} DAGs, "
            f"{num_tasks} tasks/DAG ({topology})"
        )


if __name__ == "__main__":
    base = "bench/airflow_dags"

    # Airflow official-aligned scenarios.
    setup_scenario(base, "scene1_linear_100x10", 100, 10, "linear")
    setup_scenario(base, "scene2_linear_10x100", 10, 100, "linear")
    setup_scenario(base, "scene3_tree_100x10", 100, 10, "tree")

    # Real-world topology sweeps.
    setup_scenario(base, "scene7_diamond_100x10", 100, 10, "diamond")
    setup_scenario(base, "scene8_fanout_100x10", 100, 10, "fanout")
    setup_scenario(base, "scene9_fanin_100x10", 100, 10, "fanin")
    setup_scenario(base, "scene10_mesh_100x10", 100, 10, "mesh")

    # Burst-ready sweep: one root completion releases N tasks at once.
    setup_scenario(
        base,
        "scene4_burst_ready_1x100",
        1,
        0,
        "burst_ready",
        burst_width=100,
    )
    setup_scenario(
        base,
        "scene5_burst_ready_1x500",
        1,
        0,
        "burst_ready",
        burst_width=500,
    )
    setup_scenario(
        base,
        "scene6_burst_ready_1x1000",
        1,
        0,
        "burst_ready",
        burst_width=1000,
    )

import os


def _first_present(*names, default=None):
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return value
    return default


def get_bench_db_host():
    return _first_present("DAGFORGE_BENCH_DB_HOST", "DAGFORGE_DB_HOST", default="127.0.0.1")


def get_bench_db_port():
    return int(
        _first_present("DAGFORGE_BENCH_DB_PORT", "DAGFORGE_DB_PORT", default="3306")
    )


def get_bench_db_user():
    return _first_present("DAGFORGE_BENCH_DB_USER", "DAGFORGE_DB_USERNAME", default="root")


def get_bench_db_password():
    password = _first_present("DAGFORGE_BENCH_DB_PASS", "DAGFORGE_DB_PASSWORD")
    if password is None:
        raise RuntimeError(
            "database password is not configured; export DAGFORGE_BENCH_DB_PASS "
            "or DAGFORGE_DB_PASSWORD before running bench scripts"
        )
    return password


def get_bench_db_name():
    return _first_present(
        "DAGFORGE_BENCH_DB_NAME",
        "DAGFORGE_DB_DATABASE",
        default="dagforge_perf16",
    )


def open_bench_db_connection(pymysql_module):
    return pymysql_module.connect(
        host=get_bench_db_host(),
        port=get_bench_db_port(),
        user=get_bench_db_user(),
        password=get_bench_db_password(),
        database=get_bench_db_name(),
    )


def apply_dagforge_db_env(env):
    merged = dict(env)

    def set_if_missing(name, value):
        if not merged.get(name):
            merged[name] = value

    set_if_missing("DAGFORGE_DB_HOST", get_bench_db_host())
    set_if_missing("DAGFORGE_DB_PORT", str(get_bench_db_port()))
    set_if_missing("DAGFORGE_DB_USERNAME", get_bench_db_user())
    set_if_missing("DAGFORGE_DB_PASSWORD", get_bench_db_password())
    set_if_missing("DAGFORGE_DB_DATABASE", get_bench_db_name())
    return merged

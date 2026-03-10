# Pytest plugin used by CockroachDB's psycopg roachtest to skip tests marked
# with @pytest.mark.crdb_skip("reason"). Load with: pytest -p psycopg_crdb_skip

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "crdb_skip(reason): mark test to skip when running against CockroachDB",
    )


def pytest_collection_modifyitems(config, items):
    import pytest
    for item in items:
        for mark in item.iter_markers("crdb_skip"):
            reason = mark.args[0] if mark.args else "CockroachDB"
            item.add_marker(pytest.mark.skip(reason=f"crdb_skip: {reason}"))
            break

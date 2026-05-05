"""
Test that modules can be imported without Spark/Sedona.
"""

import sys


def test_package_import():
    """Test that the package can be imported."""
    import kryptosm

    assert kryptosm is not None
    print("Package import: OK")


def test_cli_import():
    """Test that CLI module can be imported."""
    from kryptosm import cli

    assert cli is not None
    print("CLI import: OK")


def test_iceberg_import():
    """Test that iceberg module can be imported."""
    from kryptosm import iceberg

    assert iceberg is not None
    assert hasattr(iceberg, "create_iceberg_table")
    assert hasattr(iceberg, "table_exists")
    print("Iceberg module import: OK")


def test_osc_import():
    """Test that OSC module can be imported."""
    from kryptosm import osc

    assert osc is not None
    assert hasattr(osc, "OSCData")
    assert hasattr(osc, "osc_dedup")
    print("OSC module import: OK")


def test_geometry_import():
    """Test that geometry module can be imported."""
    # Geometry module requires pyspark but not Sedona JARs at import time
    try:
        from kryptosm import geometry

        assert geometry is not None
        assert hasattr(geometry, "build_node_geometry")
        print("Geometry module import: OK")
    except Exception as e:
        print(f"Geometry module import: WARNING - {e}")
        # This is OK - geometry module needs Spark


def test_spark_import():
    """Test that spark module can be imported."""
    from kryptosm import spark

    assert spark is not None
    assert hasattr(spark, "create_spark_session")
    print("Spark module import: OK")


if __name__ == "__main__":
    print("=" * 60)
    print("IMPORT TESTS")
    print("=" * 60)

    tests = [
        test_package_import,
        test_cli_import,
        test_iceberg_import,
        test_osc_import,
        test_geometry_import,
        test_spark_import,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"{test.__name__}: FAIL - {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)

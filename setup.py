from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="airflow-provider-duckdb",
    version="0.0.2",
    description="DuckDB (duckdb.org) provider for Apache Airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=duckdb_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=["duckdb_provider", "duckdb_provider.hooks"],
    install_requires=["apache-airflow>=2.0", "duckdb>=0.5.0", "duckdb-engine"],
    setup_requires=["setuptools", "wheel"],
    author="Julian LaNeve",
    author_email="julian@astronomer.io",
    url="http://astronomer.io/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.7",
)

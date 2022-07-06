from setuptools import find_packages
from setuptools import setup

setup(
    name="pgbelt",
    version="0.1.1",
    packages=[p for p in find_packages() if not p.startswith("tests")],
    install_requires=[
        "typer>=0.3.2",
        "asyncpg>=0.24.0",
        "pydantic>=1.8.2",
        "tabulate>=0.8.9",
        "aiofiles>=0.7.0",
    ],
    python_requires=">=3.9.4",
    entry_points={"console_scripts": ["belt = pgbelt.main:app"]},
)

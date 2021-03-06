from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()


setup(
    name="farmsubsidy-store",
    version="0.2",
    description="Importer, storage and api for farmsubsidy data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Simon Wörpel",
    author_email="simon.woerpel@medienrevolte.de",
    url="https://github.com/okfde/farmsubsidy-store",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    packages=find_packages(),
    package_dir={"farmsubsidy": "farmsubsidy_store"},
    install_requires=[
        "banal",
        "cachelib",
        "Click",
        "clickhouse-driver",
        "countrynames",
        "duckdb",
        "fastapi",
        "fingerprints",
        "followthemoney",
        "furl",
        "matplotlib",
        "pandas",
        "pydantic",
        "pyicu",
        "redis",
        "hiredis",
        "structlog",
    ],
    entry_points={
        "console_scripts": ["fscli = farmsubsidy_store.cli:cli"],
    },
)

from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()


setup(
    name="farmsubsidy-store",
    version="0.1",
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
        "Click",
        "countrynames",
        "duckdb",
        "fingerprints",
        "followthemoney",
        "pandas",
        "pydantic",
        "pyicu",
        "structlog",
    ],
    entry_points={
        "console_scripts": ["fscli = farmsubsidy_store.cli:cli"],
    },
)

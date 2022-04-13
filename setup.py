from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()


setup(
    name="farmsubsidy-import",
    version="0.1",
    description="Importer for farmsubsidy data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Simon Wörpel",
    author_email="simon.woerpel@medienrevolte.de",
    url="https://github.com/okfde/farmsubsidy-import",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
    package_dir={"farmsubsidy": "farmsubsidy_import"},
    install_requires=[
        "Click",
        "countrynames",
        "duckdb",
        "fingerprints",
        "followthemoney",
        "pandas",
        "structlog",
    ],
    entry_points={
        "console_scripts": ["fscli = farmsubsidy_import.cli:cli"],
    },
)

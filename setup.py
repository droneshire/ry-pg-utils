from setuptools import find_packages, setup

required = []

with open("packages/base_requirements.in", encoding="utf-8") as infile:
    required = [line.strip() for line in infile if line.strip() and not line.startswith("#")]

LONG_DESCRIPTION = ""
with open("README.md", encoding="utf-8") as infile:
    LONG_DESCRIPTION = infile.read()

VERSION = "0.0.0"
with open("VERSION", encoding="utf-8") as infile:
    VERSION = infile.read().strip()

setup(
    name="ry-pg-utils",
    version=VERSION,
    description="Utility functions for PostgreSQL",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Ross Yeager",
    author_email="ryeager12@email.com",
    package_dir={"": "src"},
    packages=find_packages(where="src", include=["ry_pg_utils", "ry_pg_utils.*"]),
    package_data={"ry_pg_utils": ["py.typed"]},
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
    ],
)

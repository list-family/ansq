import sys

from setuptools import find_packages, setup

PY_VER = sys.version_info

# NOTE: keep this in sync with `additional_dependencies` in `.pre-commit-config.yaml`
install_requires = [
    "aiohttp>=1.0.0",
    "attrs>=20.1",
]

classifiers = [
    "License :: OSI Approved :: MIT License",
    "Development Status :: 3 - Alpha",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "Topic :: Software Development",
    "Topic :: Software Development :: Libraries",
]

with open("README.md") as f:
    long_description = f.read()

setup(
    name="ansq",
    version="0.0.18",
    description="Written with native Asyncio NSQ package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
    classifiers=classifiers,
    author="LiST",
    author_email="support@list.family",
    url="https://github.com/list-family/ansq",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    install_requires=install_requires,
    include_package_data=True,
)

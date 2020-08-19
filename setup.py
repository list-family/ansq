import sys

from setuptools import find_packages, setup

from ansq import __version__

PY_VER = sys.version_info
install_requires = [
    "aiohttp>=1.0.0",
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
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "Topic :: Software Development",
    "Topic :: Software Development :: Libraries",
]

with open("README.md") as f:
    long_description = f.read()

setup(
    name="ansq",
    version=__version__.__version__,
    description="Written with native Asyncio NSQ package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=classifiers,
    author="LiST",
    author_email="support@list.family",
    url="https://github.com/list-family/ansq",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    install_requires=install_requires,
    include_package_data=True,
)

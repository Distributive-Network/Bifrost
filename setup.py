import setuptools
from setuptools import find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setuptools.setup(
    name="Bifrost",
    version="0.5.3",
    author="Kings Distributed Systems",
    author_email="toolchains@kingsds.network",
    description="Python to JS intercommunication and execution",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Kings-Distributed-Systems/Bifrost",
    packages=find_packages(),
    package_data={
        '': ['*.js','*.json']
    },
    include_package_data=False,
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
    install_requires=[
        "cloudpickle",
        "numpy",
        "xxhash",
        "posix_ipc ; os_name == 'posix' and python_version < '3.8'",
    ],
    python='>=3.6'
)

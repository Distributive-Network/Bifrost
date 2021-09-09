# MODULES

# pypi modules
import setuptools

# PROGRAM

with open("./README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Bifrost",
    version="0.0.1",
    author="Hamada Gasmallah",
    author_email="hamada@kingsds.network",
    description="Python to JS intercommunication and execution",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Kings-Distributed-Systems/Bifrost",
    packages=setuptools.find_packages(),
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
        "posix_ipc",
        "xxhash"
    ],
    python='>=3.6'
)

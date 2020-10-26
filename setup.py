import os, sys
from setuptools import setup, find_packages

#def _post_install(dir):
#    from subprocess import call
#    cwd = os.getcwd()
#    return
#
#
#class install(_install):
#    def run(self):
#        _install.run(self)
#        self.execute(_post_install, (self.install_lib,),
#                     msg="Running post install task")


def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join(path, filename))
    return paths


setup(
    name="Bifrost",
    version="0.0.1",
    author="Hamada Gasmallah",
    author_email="hamada@kingsds.network",
    description="A bridge between two languages",
    long_description="Allows for intercommunication between python and node environments",
    long_description_content_type="text/markdown",
    install_requires=[
        'numpy',
        'posix_ipc',
        'xxhash'
    ],
    package_data={
        '': ['js/main.js','js/utils.js']
    },
    packages=find_packages(),
    include_package_data=False,
    zip_safe=False
)

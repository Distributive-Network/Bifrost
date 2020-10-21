import os, sys
from distutils.core import setup
from distutils.command.install import install as _install

def _post_install(dir):
    from subprocess import call
    cwd = os.getcwd()
    return


class install(_install):
    def run(self):
        _install.run(self)
        self.execute(_post_install, (self.install_lib,),
                     msg="Running post install task")



setup(
    name="Bifrost",
    version="0.0.1",
    author="Hamada Gasmallah",
    author_email="hamada@kingsds.network",
    description="A bridge between two languages",
    long_description="Allows for intercommunication between python and node environments",
    long_description_content_type="text/markdown",
    cmdclass = {'install': install},
    packages=['bifrost'],
    install_requires=[
        'numpy',
        'posix_ipc'
    ]
    python_requires='>=3.6',
)
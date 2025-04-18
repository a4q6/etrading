from setuptools import setup, find_packages

def _requires_from_file(filename):
    return open(filename).read().splitlines()

setup(
    name="etr",
    version="0.0.0",
    packages=find_packages("src"),
    package_dir={'': 'src'},
    # install_requires=_requires_from_file('requirements.txt'),
)

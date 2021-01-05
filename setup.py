import setuptools
import os

local_path = os.path.dirname(__file__)
# Fix for tox which manipulates execution pathing
if not local_path:
    local_path = '.'
here = os.path.abspath(local_path)


def read(fname):
    with open(fname, 'r') as fhandle:
        return fhandle.read()


def version():
    with open(here + '/sdf/_version.py', 'r') as ver:
        for line in ver.readlines():
            if line.startswith('version ='):
                return line.split(' = ')[-1].strip()[1:-1]
    raise ValueError('No version found in sdf/_version.py')


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

requirements = read(os.path.join(os.path.dirname(__file__), "requirements.txt"))


setuptools.setup(
    name="sankir-sdf",
    version=version(),
    author="SanKir Technologies Pvt Ltd",
    author_email="sanjay@sankir.com",
    description="SDF Converter",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SanKirTech/sdf-converter",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "sankir-sdf=sdf.__main__:main"
        ]
    },
    install_requires=requirements,
    python_requires='>=3.6',
)
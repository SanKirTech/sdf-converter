import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sdf-sankir",
    version="0.0.1",
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
    python_requires='>=3.6',
)
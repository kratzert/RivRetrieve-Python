"""Setup script for the RivRetrieve package."""

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="rivretrieve",
    version="0.1.0",
    author="RivRetrieve Python Contributors",
    author_email="f.kratzert@gmail.com",  # Replace with a valid email
    description="A Python package for retrieving global river gauge data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your_username/RivRetrieve-Python",  # Replace with actual URL
    packages=find_packages(),
    include_package_data=True,
    package_data={"rivretrieve": ["cached_site_data/*.csv"]},
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Hydrology",
    ],
    python_requires=">=3.10",
)

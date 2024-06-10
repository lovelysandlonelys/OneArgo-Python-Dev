# A template taken from online, will likely need to revise and update when we get
# to officially packaging.
from setuptools import setup, find_packages

setup(
    name="oneargopy",
    version="1.0",
    author="Savannah Stephenson",
    author_email="savannah.stephenson@noaa.gov",
    description="A package for downloading and handling data from the Argo GDAC",
    packages=find_packages(),
    install_requires=[
        "requests",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: ",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

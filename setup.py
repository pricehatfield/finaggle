from setuptools import setup, find_packages

setup(
    name="local_reconcile",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy",
        "pytest",
    ],
    author="Price Hatfield",
    description="A tool for reconciling financial transactions from various sources",
    python_requires=">=3.6",
) 
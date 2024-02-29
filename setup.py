from setuptools import find_packages, setup

setup(
    name="CTRAN",
    packages=find_packages(exclude=["CTRAN_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
	"oracledb",
	"pymssql",
        "pyodbc",
        "pandas",
        "polars",
        "numpy",
        "pandera",
        "requests",
        "json",
        "fabric",
        "smtplib",
        "email",
        "dagster-webserver"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

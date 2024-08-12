from setuptools import find_packages, setup

setup(
    name="etl_pipeline",
    packages=find_packages(),
    install_requires=[
        "dagster==1.7.2",
        "dagster-cloud",
        "dagster-dbt==0.23.2",
        "dagster-airbyte"
    ],
    extras_require={"dev": ["dagster-webserver"]},
)

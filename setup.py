from setuptools import setup, find_packages

setup(
    name="SnowCLI",
    version="1.0.0",
    description="Snowflake utility library",
    author="Brian Kalinowski",
    url="",
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8"
    ],
    py_modules=["snow"],
    install_requires=[
        "PyYAML==6.0.0",
        "snowflake-snowpark-python==1.4.0",
        "boto3==1.15.18",
        "click==8.1.3",
        "pydantic==1.9.1",
        "pydantic-yaml==0.8.0",
        "cryptography==3.2.1"
    ],
    extras_require={
        "test": [
            "pytest==7.1.2"
        ]
    },
    entry_points="""
        [console_scripts]
        snow=snow:entry_point
    """,
    packages=find_packages(),
    package_data={
        "test": ["tests/test_resources/*"]
    },
    include_package_data=True
)

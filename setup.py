from setuptools import setup

if __name__ == "__main__":
    setup(
        packages=["datatools"],
        keywords=[],
        install_requires=[
            "appdirs",
            "click",
            "coloredlogs",
            "jsonpath-ng>=1.5",
            "requests",
            "tzlocal",
            "unidecode",
            "sqlalchemy>=2.0",
            "sqlparse",
            "pyodbc",
            "numpy",
            "pandas",
            "chardet",
            "frictionless<5",  # frictionless no longer works with python3.7
            "jsonschema",
            "xarray",
        ],
        install_recommends=[  # this does not do anything, just for information
            "geopandas",
            "h5netcdf",
            "scipy",
            "lxml",
            "rioxarray",
            "beautifulsoup4",
            "openpyxl",
            "rioxarray"
            # "pyproj",
            # "gdal",
        ],
        name="wingechr-datatools",
        description=None,
        long_description=None,
        long_description_content_type="text/markdown",
        version="0.4.0",
        author="Christian Winger",
        author_email="c@wingechr.de",
        url="https://github.com/wingechr/datatools",
        platforms=["any"],
        license="Public Domain",
        project_urls={"Bug Tracker": "https://github.com/wingechr/datatools"},
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
            "Operating System :: OS Independent",
        ],
        entry_points={"console_scripts": ["datatools = datatools.__main__:main"]},
    )

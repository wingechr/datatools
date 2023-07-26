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
            "pandas",
        ],
        name="wingechr-datatools",
        description=None,
        long_description=None,
        long_description_content_type="text/markdown",
        version="0.1.1",
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

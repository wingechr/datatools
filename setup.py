from setuptools import setup

if __name__ == "__main__":
    setup(
        packages=["datatools"],
        keywords=[],
        install_requires=[
            "appdirs",
            "click",
            "coloredlogs",
            # "filelock",
            # "frictionless>=5.10",
            "jsonpath-ng>=1.5",
            # "jsonref>=1.1",
            # "jsonschema>=4.17",
            # "pandas>=1.5",
            # "pyyaml",
            "requests",
            # "pytz"
            "tzlocal",
            "unidecode",
        ],
        name="datatools",
        description=None,
        long_description=None,
        long_description_content_type="text/markdown",
        version="0.1.0",
        author="Christian Winger",
        author_email="c@wingechr.de",
        url="https://github.com/wingechr/datatools",
        platforms=["any"],
        license="Public Domain",
        project_urls={"Bug Tracker": "https://github.com/wingechr/datatools"},
        classifiers=[
            "Programming Language :: Python :: 3.9",
            "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
            "Operating System :: OS Independent",
        ],
        entry_points={"console_scripts": ["datatools = datatools.__main__:main"]},
    )

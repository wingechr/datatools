import re
from setuptools import setup, find_packages

if __name__ == "__main__":

    setup(
        packages=["datatools"],
        keywords=[],
        install_requires=[
            "unidecode",
            "click",
            "coloredlogs",
            "networkx",
            "chardet",
            "pandas",
            "numpy",
            "scipy"
        ],        
        name="datatools",
        description="",  # should be one line
        long_description="",
        long_description_content_type="text/markdown",  # text/markdown or text/x-rst or text/plain
        version="0.0.0",
        #author=pkg.__author__,
        #author_email=pkg.__email__,
        #maintainer=pkg.__author__,
        #maintainer_email=pkg.__email__,
        #url=pkg.__url__,
        #download_url=pkg.__url__,
        platforms=["any"],
        license="Public Domain",
        #project_urls={"Bug Tracker": pkg.__url__,},
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
            "Operating System :: OS Independent",
        ],
        entry_points={
            # "console_scripts": ["cmd = PACKAGE_NAME.scripts.NAME:main"]
        },
        package_data={
            # "package.module": [file_patterns]
        },
    )

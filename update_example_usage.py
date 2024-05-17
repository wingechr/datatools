"""Script to update the README sections:

- Example usage from `docs/example_usage.py`
- main script cli help from `python -m datatools help-all`

"""

import re

from datatools.__main__ import _recursive_help

path_py = "docs/example_usage.py"
path_readme = "README.md"
encoding = "utf-8"
section_readme = "## Example usage"
section_cli = "## Command line"


def replace_md_section(md_template: str, md_section: str, md_data: str) -> str:
    # very simple implementation
    pmatch = rf"{md_section}\s+```[^`]+```"  # readme_section
    psub = md_data
    assert re.match(".*" + pmatch, md_template, re.DOTALL | re.MULTILINE), pmatch
    return re.sub(pmatch, psub, md_template, re.DOTALL | re.MULTILINE)


# get CLI --help text
cli_help = _recursive_help()

with open(path_py, "r", encoding=encoding) as file:
    py_code = file.read()

with open(path_readme, "r", encoding=encoding) as file:
    md_readme = file.read()

py_md_readme = f"""
{section_readme}

```python
{py_code}
```
"""

cli_md_readme = f"""
{section_cli}

```bash
{cli_help}
```
"""


md_readme = replace_md_section(
    md_template=md_readme, md_section=section_readme, md_data=py_md_readme
)


with open(path_readme, "w", encoding=encoding) as file:
    file.write(md_readme)

import os
import sys

sys.path.insert(0, os.path.abspath("../"))

project = "RivRetrieve-Python"
copyright = "2025, Frederik Kratzert"
author = "Frederik Kratzert"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_rtd_theme",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

autodoc_default_options = {
    "members": True,
    "inherited-members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

import re
from rivretrieve import constants


def autodoc_process_docstring(app, what, name, obj, options, lines):
    if not lines:
        return

    new_lines = []

    for line in lines:
        matches = re.findall(r"constants\.([A-Z_]+)", line)

        for match in matches:
            if hasattr(constants, match):
                const_val = getattr(constants, match)

                line = line.replace(f"constants.{match}", f"'{const_val}'")

        new_lines.append(line)

    lines[:] = new_lines


def autodoc_skip_member(app, what, name, obj, skip, options):
    # Skip class attributes that are all uppercase (likely constants)
    if what == "class" and name.isupper() and not callable(obj):
        return True
    return skip


def setup(app):
    app.connect("autodoc-process-docstring", autodoc_process_docstring)
    app.connect("autodoc-skip-member", autodoc_skip_member)

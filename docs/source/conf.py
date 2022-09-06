# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../../src/"))
sys.path.insert(0, os.path.abspath("../../examples/"))

# -- Project information -----------------------------------------------------

project = "NEMED"
copyright = "2022, Declan Heim, Shayan Naderi"
author = "Declan Heim, Shayan Naderi"

# The full version, including alpha/beta/rc tags
release = "0.1.0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

# autodoc allows docstrings to be pulled straight from your code
# napoleon supports Google/NumPy style docstrings
# intersphinx can link to other docs, e.g. standard library docs for try:
# doctest enables doctesting
# todo is self explanatory
# viewcode adds links to highlighted source code
# MyST is a CommonMark parser that plugs into Sphinx. Enables you to write docs in md.
# MyST-nb allows inclusion of jupyter notebooks
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.doctest",
    "sphinx.ext.todo",
    "sphinx_copybutton",
    "sphinx_external_toc",
    "myst_nb"
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Formats for MyST --------------------------------------------------------
source_suffix = [".rst", ".md"]
nb_custom_formats = {
    ".md": ["jupytext.reads", {"fmt": "mystnb"}],
}

# --  Napoleon options--------------------------------------------------------
# use the :param directive
napoleon_use_param = True

# -- Autodoc options ---------------------------------------------------------

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
autodoc_typehints = "both"

# Only insert class docstring
autoclass_content = "class"

# --  Intersphinx options-----------------------------------------------------
intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# --  MyST options------------------------------------------------------------
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

# -- External TOC------------------------------------------------------------
external_toc_path = "_toc.yml"  # optional, default: _toc.yml
external_toc_exclude_missing = False  # optional, default: False

# --  Todo options------------------------------------------------------------
todo_include_todos = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_book_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_show_sourcelink = False
html_theme_options = {
    "repository_url": "https://github.com/dec-heim/NEMGLO",
    "use_repository_button": True,
    "use_issues_button": True,
    }

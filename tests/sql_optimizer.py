"""
SQL Optimizer App

This Streamlit app allows users to optimize SQL queries using predefined optimization rules.
Users can select optimization rules, choose to remove common table expressions (CTEs), and
optionally format the optimized query using sqlfmt. The original and optimized queries are
displayed side by side for comparison. Additionally, users can view the source code on GitHub.

Author: [Your Name]

"""
from typing import Callable, Dict, Sequence

from sqlfmt.api import Mode, format_string
from sqlglot import parse_one
from sqlglot.expressions import Select
from sqlglot.optimizer import RULES, optimize
import streamlit as st
from streamlit_ace import st_ace

RULE_MAPPING: Dict[str, Callable] = {rule.__name__: rule for rule in RULES}
SAMPLE_QUERY: str = """WITH users AS (
    SELECT *
    FROM users_table),
orders AS (
    SELECT *
    FROM orders_table),
combined AS (
    SELECT users.id, users.name, orders.order_id, orders.total
    FROM users
    JOIN orders ON users.id = orders.user_id)
SELECT combined.id, combined.name, combined.order_id, combined.total
FROM combined
"""


def _generate_ast(query: str) -> Select:
    """
    Generate an AST from a query.
    """
    ast = parse_one(query)
    return ast


def apply_optimizations(
    query: str, rules: Sequence[Callable] = RULES, remove_ctes: bool = False
) -> Select:
    """
    Apply optimizations to an AST.
    """
    ast = _generate_ast(query)
    if remove_ctes:
        return optimize(ast, rules=rules)
    else:
        return optimize(ast, rules=rules, leave_tables_isolated=True)


def format_sql_with_sqlfmt(query: str) -> str:
    """
    Format a query using sqlfmt.
    """
    mode = Mode()
    return format_string(query, mode)

# Set custom Streamlit page configuration
st.set_page_config(
    page_title="SQL Optimizer",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Hide Streamlit default menu and footer
hide_st_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)

# Custom CSS styling
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600&display=swap');

    body {
        font-family: 'Open Sans', sans-serif;
        background-color: #f8f9fa; /* Light background */
        color: #333; /* Dark grey text */
    }

    .stButton {
        background-color: black; /* Primary color */
        color: #fff; /* White text */
        padding: 10px 24px;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        text-align: center;
        transition: background-color 0.3s ease; /* Smooth color transition */
    }

    .stButton:hover {
        background-color: #0056b3; /* Darker hover color */
    }

    /* Editor styling */
    .ace_editor {
        border-radius: 4px; /* Rounded corners */
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); /* Add subtle box shadow */
    }

    /* Error message styling */
    .stMarkdown.stException {
        padding: 1rem; /* Add padding to error messages */
        background-color: #f8d7da; /* Light red background */
        color: #721c24; /* Dark red text */
        border-radius: 4px; /* Rounded corners */
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); /* Add subtle box shadow */
    }

    /* Success message styling */
    .stMarkdown.stSuccess {
        padding: 1rem; /* Add padding to success messages */
        background-color: #d4edda; /* Light green background */
        color: #155724; /* Dark green text */
        border-radius: 4px; /* Rounded corners */
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); /* Add subtle box shadow */
    }

    /* GitHub link styling */
    .github-link {
        text-align: center;
        margin-top: 2rem;
    }

    /* Custom GitHub button styling */
.github-btn-white-border {
    background-color: white; /* White background */
    color: black; /* Black text */
    border: 2px solid white; /* White border */
}

.github-btn-white-border:hover {
    color: #0056b3; /* Darker hover color */
    border-color: #0056b3; /* Darker border color on hover */
}

    </style>
    """,
    unsafe_allow_html=True,
)

# Title container
st.markdown(
    """
    <div style="text-align: center;">
        <h1>SQL Optimizer</h1>
    </div>
    """,
    unsafe_allow_html=True,
)

# Rule selector
selected_rules = st.multiselect(
    "Optimization rules:",
    list(RULE_MAPPING.keys()),
    default=list(RULE_MAPPING.keys()),
    key="rules_multiselect",
)

# Checkboxes
cols = st.columns(2)
remove_ctes = cols[0].checkbox(
    "Remove CTEs", on_change=None, key="remove_ctes_checkbox"
)
format_with_sqlfmt = cols[1].checkbox(
    "Lint with sqlfmt", on_change=None, key="format_with_sqlfmt_checkbox"
)

# Initialize session state
if "new_query" not in st.session_state:
    st.session_state.new_query = ""
if "state" not in st.session_state:
    st.session_state.state = 0


# Input editor
def _generate_editor_widget(value: str, **kwargs) -> str:
    return st_ace(
        value=value,
        height=300,
        theme="twilight",
        language="sql",
        font_size=16,
        wrap=True,
        auto_update=True,
        **kwargs,
    )


left, right = st.columns(2)

with left:
    sql_input = _generate_editor_widget(SAMPLE_QUERY, key="input_editor")

# Optimize and lint query
if st.button("Optimize SQL", key="optimize_button"):
    try:
        rules = [RULE_MAPPING[rule] for rule in selected_rules]
        new_query = apply_optimizations(sql_input, rules, remove_ctes).sql(pretty=True)
        if format_with_sqlfmt:
            new_query = format_sql_with_sqlfmt(new_query)
        st.session_state.new_query = new_query
        st.session_state.state += 1
        st.success("SQL query optimized successfully!")

    except Exception as e:
        st.error(f"Error: {e}")

# CSS for the button
css = """
<style>
.stButton {
    background-color: #4CAF50;
    border: none;
    color: white;
    padding: 15px 32px;
    text-align: center;
    text-decoration: none;
    display: inline-block;
    font-size: 16px;
    margin: 4px 2px;
    cursor: pointer;
}
</style>
"""

# Add the HTML button for optimization with CSS
st.markdown(css, unsafe_allow_html=True)

# Output editor
with right:
    _generate_editor_widget(
        st.session_state.new_query, readonly=True, key=f"ace-{st.session_state.state}"
    )

# Include Font Awesome CSS
st.markdown(
    """
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css" rel="stylesheet">
    """,
    unsafe_allow_html=True,
)

# GitHub link
st.markdown(
    """
    <div class="github-link">
        <a href="https://github.com/shubhusion/sql-optimizer-app-main" target="_blank" class=".github-btn-white-border">
            <i class="fab fa-github"></i> View on GitHub
        </a>
    </div>
    """,
    unsafe_allow_html=True,
)
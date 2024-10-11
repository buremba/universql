import streamlit as st
import sqlglot
from sqlglot import expressions
import networkx as nx
from st_link_analysis import st_link_analysis, NodeStyle

example_sql = """\
CREATE VIEW sales.customer_order_total_percentage AS
SELECT
    order.id,
    email,
    order.total_amount,
    total_spent,
    100 * order.total_amount / total_spent AS order_total_percentage
FROM sales.customer_order_summary
JOIN sales.order
    ON customer_order_summary.order_id = order.id;"""

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title="sqlglot Abstract Syntax Tree (AST) Viewer",
    page_icon=":deciduous_tree:",
    layout="wide",
)

# Set the title that appears at the top of the page.
"""
# :deciduous_tree: sqlglot Abstract Syntax Tree (AST) Viewer

View the abstract syntax tree generated by sqlglot for SQL code.

If there are multiple statements, only the first statement will be visualized.
"""

# Add some spacing
""
""

sql = st.text_area("Input SQL code here:", value=example_sql, height=300)

""
""
""

parsed_sql = sqlglot.parse(sql)


def is_flattenable(node, classes):
    return isinstance(node, classes)


def ast_to_digraph(ast, flattenable_classes):
    """
    Convert a SQLGlot AST into a NetworkX DiGraph.
    Args:
        ast (sqlglot.Expression): The root of the SQLGlot AST.
    Returns:
        nx.DiGraph: A directed graph representing the AST.
    """
    graph = nx.DiGraph()

    def add_node_and_edges(node, parent=None):
        """
        Recursively add nodes and edges to the graph based on the AST.
        Args:
            node (sqlglot.Expression): The current AST node.
            parent (str): The label of the parent node, if any.
        """
        # Create a unique identifier for each node using its type and id

        node_id = id(node)
        node_label = f"{node.__class__.__name__}"
        node_kind = (
            f"({node.args.get('kind')})" if node.args.get("kind") else ""
        )
        node_name = f"{node_label} {node_kind}".strip()

        all_children_identifiers = all(
            [
                isinstance(x, expressions.Identifier)
                for x in filter(None, node.args.values())
            ]
        )
        flattenable = (
            is_flattenable(node, flattenable_classes)
            or all_children_identifiers
        )

        if flattenable or node.is_leaf():
            node_content = node.sql()
        else:
            node_content = (
                node.this if isinstance(node, expressions.Identifier) else ""
            )

        # Add the node to the graph with its label
        graph.add_node(
            node_id,
            label=node_label,
            name=node_name,
            kind=node_kind,
            content=node_content,
        )
        # If there is a parent node, add an edge from parent to current node
        if parent is not None:
            graph.add_edge(parent, node_id)

        # Recursively process child nodes
        for child in node.args.values():
            if isinstance(child, list) and not flattenable:
                for sub_child in child:
                    if isinstance(sub_child, sqlglot.Expression):
                        add_node_and_edges(sub_child, node_id)
            elif isinstance(child, sqlglot.Expression) and not flattenable:
                add_node_and_edges(child, node_id)

    # Start traversal from the root of the AST
    add_node_and_edges(ast)

    return graph


def graph_to_elements(graph):
    """
    Convert a NetworkX DiGraph into the specified elements data structure.

    Args:
        graph (nx.DiGraph): The directed graph to convert.

    Returns:
        dict: A dictionary with nodes and edges formatted according to the
          specified structure.
    """
    elements = {"nodes": [], "edges": []}

    # Convert nodes to the desired format
    for node_id, node_data in graph.nodes(data=True):
        # Extract the node label and other attributes
        node_label = node_data.get("label", "NODE")
        node_name = node_data.get("name", "")
        node_content = node_data.get("content", "")

        # Customize the node data format based on available attributes
        node_entry = {"data": {"id": node_id, "label": node_label}}
        if node_name:
            node_entry["data"]["name"] = node_name
        if node_content:
            node_entry["data"]["content"] = node_content

        elements["nodes"].append(node_entry)

    # Convert edges to the desired format
    for edge_id, (source, target, edge_data) in enumerate(
        graph.edges(data=True), start=1
    ):
        edge_label = edge_data.get("label", "EDGE")

        # Create edge data with source, target, and attributes
        edge_entry = {
            "data": {
                "id": edge_id,
                "label": edge_label,
                "source": source,
                "target": target,
            }
        }
        elements["edges"].append(edge_entry)

    return elements


def ast_to_link_analysis(ast, classes):
    graph = ast_to_digraph(ast, classes)
    elements = graph_to_elements(graph)
    st_link_analysis(
        elements,
        "breadthfirst",
        [
            NodeStyle("Select", "#FF5733", "content"),
            NodeStyle("Create", "#33FF57", "content"),
            NodeStyle("With", "#3357FF", "content"),
            NodeStyle("Table", "#FF33A1", "content", "table"),
            NodeStyle("Column", "#33FFA1", "content", "view_column"),
            NodeStyle("Alias", "#4340A1", "content"),
            NodeStyle("ColumnDef", "#33FFA1", "content"),
            NodeStyle("ColumnConstraint", "#A133FF", "content"),
            NodeStyle("Delete", "#FF8C33", "content"),
            NodeStyle("DropCopy", "#8CFF33", "content"),
            NodeStyle("ForeignKey", "#33FF8C", "content"),
            NodeStyle("PrimaryKey", "#8C33FF", "content"),
            NodeStyle("Into", "#FF3333", "content"),
            NodeStyle("From", "#33FF33", "content"),
            NodeStyle("Having", "#3333FF", "content"),
            NodeStyle("Index", "#FF333F", "content"),
            NodeStyle("Insert", "#33FF3F", "content"),
            NodeStyle("Limit", "#3F33FF", "content"),
            NodeStyle("Group", "#FF33FF", "content"),
            NodeStyle("Join", "#33FFFF", "content"),
            NodeStyle("Properties", "#FFCC33", "content"),
            NodeStyle("Where", "#33CCFF", "content"),
            NodeStyle("Order", "#CC33FF", "content"),
        ],
        [],
    )


sql_expressions = {
    x.__name__: x
    for x in expressions.__dict__.values()
    if isinstance(x, expressions._Expression)
}

default_flattenable_classes = ["Column", "ColumnDef", "Alias"]

flattenable_classes = st.multiselect(
    "Select what types of expressions you would like to flatten:",
    sql_expressions.keys(),
    default=default_flattenable_classes,
)

if parsed_sql[0]:
    ast_to_link_analysis(
        parsed_sql[0],
        tuple(
            sql_expressions.get(class_name)
            for class_name in flattenable_classes
        ),
    )

    st.code(parsed_sql[0].__repr__())
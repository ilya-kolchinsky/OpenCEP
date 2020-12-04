import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

from tree.nodes.Node import Node

debug = False


def conditional_print(str):
    if debug:
        print(str)


class GraphVisualization:
    """
        Utility class for visualization of the evaluation trees and graphs
    """
    def __init__(self, title=None):
        self.vertexes = set()
        self.vertexes_positions = {}
        self.nodes_level_counter_dict = {}
        self.edges = set()
        self.title = "" if title is None else title

    def addVertex(self, v):
        """
           Adding Vertex v to the graph
        """
        self.vertexes.add(v)

    def addEdge(self, u, v):
        """
            Adding direct Vertex to the graph
        """
        temp_edge = (u, v)
        self.addVertex(u)
        self.addVertex(u)
        self.edges.add(temp_edge)

    def build_from_leaves(self, leaves):
        """
            Build graph from leaves, which for each one, traverse up to all of its ancestors
            @param : leaves, list of leaves to build graph from
        """
        for leaf in leaves:
            self.add_route_from_node_to_all_ancestors(leaf)

    def add_route_from_node_to_all_ancestors(self, node: Node, node_level=0):
        """
             recursively traverse from each node to its ancestors and adding the path
        """
        curr_node_value = str(node)
        self.addVertex(curr_node_value)
        self.set_vertex_pos(curr_node_value, node_level)

        for parent in node.get_parents():
            parent_val = str(parent)
            self.addEdge(curr_node_value, parent_val)
            self.add_route_from_node_to_all_ancestors(parent, node_level + 1)

    def set_vertex_pos(self, vertex_value, node_level: int):
        """
            auxiliary function for adjusting the node position in the layout image
            @param : leaves, list of leaves to build graph from
        """
        if node_level not in self.nodes_level_counter_dict:
            # first node at this level
            self.nodes_level_counter_dict[node_level] = 1
        else:
            self.nodes_level_counter_dict[node_level] += 1

        number_of_nodes = self.nodes_level_counter_dict[node_level]
        # self.vertexes_positions[vertex_value] = (number_of_nodes*7, node_level + 0.2 * number_of_nodes)
        # self.vertexes_positions[vertex_value] = (number_of_nodes*7, node_level+ 0.4 * number_of_nodes)
        self.vertexes_positions[vertex_value] = (number_of_nodes*7, node_level+ 0.4 * number_of_nodes)

    def visualize(self):
        """
            visualize function using networkx package
        """
        df = pd.DataFrame({'from': (edge[0] for edge in self.edges), 'to': (edge[1] for edge in self.edges)})

        nx_graph = nx.DiGraph()
        nx_graph.add_nodes_from(self.vertexes)
        G = nx.from_pandas_edgelist(df, 'from', 'to', create_using=nx.DiGraph())

        groups_by_level = ['group' + str(self.vertexes_positions[v][1] + 1) for v in self.vertexes]
        carac = pd.DataFrame({'ID': list(self.vertexes), 'vertex_color': groups_by_level})

        carac = carac.set_index('ID')
        carac = carac.reindex(G.nodes())

        vertex_pos = self.vertexes_positions

        carac['vertex_color'] = pd.Categorical(carac['vertex_color'])

        plt.axis("off")  # turn of axis
        plt.title(self.title)  # turn of axis
        nx.draw_networkx(G, pos=vertex_pos, with_labels=True, font_size=8, node_color=carac['vertex_color'].cat.codes, node_size=1500)

        plt.margins(x=0.5)
        plt.show()



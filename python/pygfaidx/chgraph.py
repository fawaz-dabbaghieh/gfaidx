import sys
import os
import re
import logging
import zlib
import pdb
from collections import deque
from .bfs import bfs
from .node_hash_index import NodeHashIndex


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

complement = str.maketrans("ACGTN", "TGCAN")
def rev_comp(seq):
    return seq[::-1].translate(complement)

class Node:
    def __init__(self, identifier):
        self.id = identifier  # size is between 28 and 32 bytes
        self.seq = ""
        self.start = set()  # 96 bytes for 4 neighbors
        self.end = set()  # 96 bytes
        self.visited = False  # 28 bytes (used for bubble and superbubble detection)
        self.chunk_id = -1
        self.tags = dict()
        self.optional_info = []

    def __len__(self):
        return len(self.seq)

    def to_gfa_line(self, with_seq=True):
        """
        returns the GFA S line for the node
        """
        if with_seq:  # in case with_seq was true but there was no seq
            if self.seq == "":
                seq = "*"
            else:
                seq = self.seq
        else:
            seq = "*"
        tags = []
        for tag_id, tag in self.tags.items():
            tags.append(f"{tag_id}:{tag[0]}:{tag[1]}")
        return "\t".join(["S", self.id, seq] + tags)


class ChGraph:
    """
    Graph object containing the important information about the graph
    """

    def __init__(self, graph_file):
        # check for index and ndx
        if not graph_file.endswith(".gfa.gz"):
            logging.error("the graph needs to end with .gfa.gz")
            sys.exit(1)

        if not os.path.exists(graph_file):
            logging.error(f"graph file {graph_file} does not exist")
            sys.exit(1)

        if not os.path.exists(graph_file + ".ndx"):
            logger.error(f"Could not find .ndx associated with {graph_file}\nMake sure this is the chunked graph")
            sys.exit(1)

        if not os.path.exists(graph_file + ".idx"):
            logger.error(f"Could not find the offsets index associated with {graph_file}\nMake sure this is the chunked graph")
            sys.exit(1)

        self.offsets = self._load_idx(graph_file + ".idx")

        self.node_index = NodeHashIndex(graph_file + ".ndx")

        self.nodes = dict()
        self.graph_name = graph_file
        self.shared_edges_by_node = {}
        self.shared_chunk_id = max(self.offsets.keys())
        self._load_shared_edges()

        self.loaded_c = deque() # newly loaded chunk IDs
        self.loaded_c_limit = 50

    def __len__(self):
        """
        overloading the length function
        """
        return len(self.nodes)

    def __str__(self):
        """
        overloading the string function for printing
        """
        total_len = sum([len(x) for x in self.nodes.values()])
        return f"The graph has {len(self.nodes)} nodes, and total seq length of {total_len}"


    def __contains__(self, key):
        """
        overloading the in operator to check if node exists in graph
        """
        return key in self.nodes

    def __getitem__(self, key):
        """
        overloading the bracket operator
        """
        try:
            return self.nodes[key]
        except KeyError:
            chunk_id = self.get_node_chunk(key)
            if chunk_id is None:
                return None
            self.load_chunk(chunk_id)
            return self.nodes[key]

    def total_seq_length(self):
        """
        returns total sequence length
        """
        total = 0
        for n in self.nodes.values():
            total += n.seq_len
        return total

    def reset_visited(self):
        """
        resets all nodes.visited to false
        """
        for n in self.nodes.values():
            n.visited = False

    def clear(self):
        """
        removes all nodes from the graph
        """
        del self.nodes
        self.nodes = dict()
        self.loaded_c = deque()

    def get_node_chunk(self, node_id):
        """
        returns the chunk id of the node using the .ndx index
        """
        return self.node_index.lookup(node_id)

    def neighbors(self, node_id):
        """
        returns all connected nodes to node_id, loads chunks if required
        """
        neighbors = []
        try:  # if not loaded, it will through KeyError
            return [x[0] for x in self.nodes[node_id].start] + [x[0] for x in self.nodes[node_id].end]
        except KeyError:
            new_chunk = self.node_index.lookup(node_id)
            if new_chunk is None:
                logger.error(f"Something went wrong, node {node_id} was not found in the index")
                logger.error(f"Please make sure you are using the correct graph and nothing has been edited")
                sys.exit()
            logger.info(f"node {node_id} is not in the graph, loading chunk {new_chunk}")
            self.load_chunk(new_chunk)
            return [x[0] for x in self.nodes[node_id].start] + [x[0] for x in self.nodes[node_id].end]

    def children(self, node_id, direction):
        """
        returns the children of a node in given direction
        """
        if node_id not in self.nodes:  # need to load a chunk
            new_chunk = self.node_index.lookup(node_id)
            self.load_chunk(new_chunk)

        if direction == 0:
            for nn in self.nodes[node_id].start:
                if nn[0] not in self.nodes:  # need to load a chunk
                    new_chunk = self.node_index.lookup(nn[0])
                    self.load_chunk(new_chunk)
            return [(x[0], x[1]) for x in self.nodes[node_id].start]
        elif direction == 1:
            for nn in self.nodes[node_id].end:
                if nn[0] not in self.nodes:
                    new_chunk = self.node_index.lookup(nn[0])
                    self.load_chunk(new_chunk)
            return [(x[0], x[1]) for x in self.nodes[node_id].end]
        else:
            raise Exception("Trying to access a wrong direction in node {}".format(self.id))

    def remove_node(self, n_id):
        """
        remove a node and its corresponding edges
        """
        starts = [x for x in self.nodes[n_id].start]
        for n_start in starts:
            overlap = n_start[2]
            if n_start[1] == 1:
                self.nodes[n_start[0]].end.remove((n_id, 0, overlap))
            else:
                self.nodes[n_start[0]].start.remove((n_id, 0, overlap))

        ends = [x for x in self.nodes[n_id].end]
        for n_end in ends:
            overlap = n_end[2]
            if n_end[1] == 1:
                self.nodes[n_end[0]].end.remove((n_id, 1, overlap))
            else:
                self.nodes[n_end[0]].start.remove((n_id, 1, overlap))

        del self.nodes[n_id]


    def write_graph(self, set_of_nodes=None,
                    output_file="output_graph.gfa",
                    append=False, optional_info=True):
        """writes a graph file as GFA
        list_of_nodes can be a list of node ids to write
        ignore_nodes is a list of node ids to not write out
        if append is set to true then output file should be an existing
        graph file to append to
        modified to output a modified graph file
        """
        if not output_file.endswith(".gfa"):
            output_file += ".gfa"

        self.write_gfa(set_of_nodes=set_of_nodes, output_file=output_file,
                       append=append)


    def bfs(self, start, size):
        """
        Returns a neighborhood of size given around start node
        :param start: starting node for the BFS search
        :param size: size of the neighborhood to return
        """
        if start not in self.nodes:
            chunk_id = self.node_index.lookup(start)
            logger.warning(f"The start node given to bfs {start} not in the graph, loading its chunk")
            self.load_chunk(chunk_id)
            # self.loaded_c.append(chunk_id)
        return bfs(self, start, size)
    # neighborhood = bfs(self, start, size)
    # return neighborhood

    # def output_chunk(self, chunk_id):
    # 	"""
    # 	This function outputs a pickled dict with the chunk's information
    # 	the chunk location should be saved with the graph location
    # 	:param chunk_id: int for the chunk id
    # 	"""
    # 	chunk = dict()
    # 	for n in self.nodes.values():
    # 		if n.chunk_id == chunk_id:
    # 			chunk[n.id] = {"seq":n.seq, "start":n.start, "end":n.end, "chunk":n.chunk, "optional_info":n.optional_info}
    # 	with open(self.graph_name + "chunk" + str(chunk_id), "wb") as outfile:
    # 		pickle.dump(chunk, outfile)

    def unload_chunk(self, chunk_id):
        """
        Unloades a loaded chunk and remove those nodes from the graph
        """
        # in unload, I need to output the chunk again
        # in case some updates been added to the chunk
        # self.output_chunk(chunk_id)
        to_remove = []
        for n in self.nodes.values():
            if n.chunk_id == chunk_id:
                to_remove.append(n.id)
        # here I am removing the nodes but not the edges associated with it
        # so in case I need to load this again in another traversal for example
        for n in to_remove:
            del self.nodes[n]
        if chunk_id in self.loaded_c:
            self.loaded_c.remove(chunk_id)

    def load_chunk(self, chunk_id):
        """
        this function will read a chunk and update the nodes in the graph
        """
        if chunk_id is None:
            logger.error("Chunk id is missing for requested node")
            return
        # print(f"loading chunk {chunk_id}")
        # with open(self.graph_name + "chunk" + str(chunk_id), "rb") as infile:
        # 	chunk = pickle.load(infile)
        if len(self.loaded_c) >= self.loaded_c_limit:
            logger.info(f"There has been {self.loaded_c_limit} chunks loaded, will be unloading old chunks!")
            while len(self.loaded_c) >= self.loaded_c_limit:
                c_id = self.loaded_c.popleft()
                logger.info(f"Unloading chunk {c_id} and current loaded c are {self.loaded_c}")
                self.unload_chunk(c_id)
        logger.info(f"Loading chunk {chunk_id}")
        offset, gz_size = self.offsets[chunk_id]
        loaded_nodes = self.read_gfa(self.graph_name, offset, gz_size)
        self._apply_shared_edges(loaded_nodes)
        if chunk_id not in self.loaded_c:
            self.loaded_c.append(chunk_id)
        logger.info(f"Loaded chunks so far {self.loaded_c}")

    # for n_id, n in chunk.items():
    # 	# to remove
    # 	if n_id not in self.nodes:
    # 		new_node = Node(n_id)
    # 		new_node.seq = n["seq"]
    # 		new_node.start = n["start"]
    # 		new_node.end = n["end"]
    # 		new_node.chunk_id = n["chunk"]
    # 		new_node.optional_info = ['optional_info']
    # 		self.nodes[n_id] = new_node
    # else:
    # 	print(f"node {n_id} already in graph, skipping...")

    def read_gfa(self, gfa_file_path, offset, gz_size):
        """
        Read a gfa file
        :param gfa_file_path: gfa graph file.
        :param low_memory: don't read the sequences to save memory
        :return: Dictionary of node ids and Node objects.
        """

        edges = []
        loaded_nodes = set()
        for line in self._iter_gzip_member_lines(gfa_file_path, offset, gz_size):
            if line.startswith("S"):
                line = line.strip().split("\t")
                n_id = str(line[1])
                n_len = len(line[2])
                self.nodes[n_id] = Node(n_id)
                self.nodes[n_id].seq = line[2]
                self.nodes[n_id].seq_len = n_len
                loaded_nodes.add(n_id)

                tags = line[3:]
                # adding the extra tags if any to the node object
                if tags:
                    for tag in tags:
                        tag = tag.split(":")
                        # I am adding the tags as key:value, key is tag_name:type and value is the value at the end
                        # e.g. SN:i:10 will be {"SN": ('i', '10')}
                        self.nodes[n_id].tags[tag[0]] = (tag[1], tag[2])  # (type, value)

            elif line.startswith("L"):
                edges.append(line)

        for e in edges:
            line = e.split()

            first_node = str(line[1])
            second_node = str(line[3])

            overlap = 0
            if len(line) > 5:
                if not line[5] == "*":
                    overlap = int(line[5][:-1])

            if line[2] == "-":
                from_start = True
            else:
                from_start = False

            if line[4] == "-":
                to_end = True
            else:
                to_end = False

            if from_start and to_end:
                if first_node in self.nodes:
                    self.nodes[first_node].start.add((second_node, 1, overlap))
                if second_node in self.nodes:
                    self.nodes[second_node].end.add((first_node, 0, overlap))
            elif from_start and not to_end:
                if first_node in self.nodes:
                    self.nodes[first_node].start.add((second_node, 0, overlap))
                if second_node in self.nodes:
                    self.nodes[second_node].start.add((first_node, 0, overlap))
            elif not from_start and not to_end:
                if first_node in self.nodes:
                    self.nodes[first_node].end.add((second_node, 0, overlap))
                if second_node in self.nodes:
                    self.nodes[second_node].start.add((first_node, 1, overlap))
            elif not from_start and to_end:
                if first_node in self.nodes:
                    self.nodes[first_node].end.add((second_node, 1, overlap))
                if second_node in self.nodes:
                    self.nodes[second_node].end.add((first_node, 1, overlap))

        # gzip stream is handled by the iterator
        return loaded_nodes

    def _load_idx(self, idx_path):
        """
        Load the .idx offsets file into a dict: community_id -> (gz_offset, gz_size)
        """
        offsets = {}
        with open(idx_path, "r") as idx_file:
            for line in idx_file:
                if line.startswith("#"):
                    continue
                parts = line.strip().split("\t")
                cid = int(parts[0])
                offsets[cid] = (int(parts[1]), int(parts[2]))
        return offsets

    def _load_shared_edges(self):
        """
        Load the shared-edge chunk (last chunk) into an in-memory index.
        """
        if self.shared_chunk_id is None:
            return
        offset, gz_size = self.offsets[self.shared_chunk_id]
        for line in self._iter_gzip_member_lines(self.graph_name, offset, gz_size):
            if not line.startswith("L"):
                continue
            edge = self._parse_edge_line(line)
            if edge is None:
                continue
            n1, _, n2, _, _ = edge
            self.shared_edges_by_node.setdefault(n1, []).append(edge)
            self.shared_edges_by_node.setdefault(n2, []).append(edge)

    def _apply_shared_edges(self, loaded_nodes):
        """
        Add shared edges involving currently loaded nodes.
        """
        for node_id in loaded_nodes:
            for edge in self.shared_edges_by_node.get(node_id, []):
                self._add_edge_tuple(edge)

    def _parse_edge_line(self, line):
        parts = line.split()
        if len(parts) < 5:
            return None
        first_node = str(parts[1])
        second_node = str(parts[3])
        overlap = 0
        if len(parts) > 5 and parts[5] != "*":
            overlap = int(parts[5][:-1])
        from_start = parts[2] == "-"
        to_end = parts[4] == "-"
        return (first_node, from_start, second_node, to_end, overlap)

    def _add_edge_tuple(self, edge):
        first_node, from_start, second_node, to_end, overlap = edge
        if from_start and to_end:
            if first_node in self.nodes:
                self.nodes[first_node].start.add((second_node, 1, overlap))
            if second_node in self.nodes:
                self.nodes[second_node].end.add((first_node, 0, overlap))
        elif from_start and not to_end:
            if first_node in self.nodes:
                self.nodes[first_node].start.add((second_node, 0, overlap))
            if second_node in self.nodes:
                self.nodes[second_node].start.add((first_node, 0, overlap))
        elif not from_start and not to_end:
            if first_node in self.nodes:
                self.nodes[first_node].end.add((second_node, 0, overlap))
            if second_node in self.nodes:
                self.nodes[second_node].start.add((first_node, 1, overlap))
        elif not from_start and to_end:
            if first_node in self.nodes:
                self.nodes[first_node].end.add((second_node, 1, overlap))
            if second_node in self.nodes:
                self.nodes[second_node].end.add((first_node, 1, overlap))

    def _iter_gzip_member_lines(self, gz_path, offset, gz_size, chunk_size=1 << 20):
        """
        Stream-decompress a single gzip member and yield lines as strings.
        """
        with open(gz_path, "rb") as fh:
            fh.seek(offset)
            remaining = gz_size
            inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
            pending = b""
            while remaining > 0:
                to_read = chunk_size if remaining > chunk_size else remaining
                data = fh.read(to_read)
                if not data:
                    break
                remaining -= len(data)
                chunk = inflater.decompress(data)
                if not chunk:
                    continue
                pending += chunk
                while True:
                    nl = pending.find(b"\n")
                    if nl == -1:
                        break
                    line = pending[:nl]
                    pending = pending[nl + 1:]
                    yield line.decode("utf-8", errors="replace")

            tail = inflater.flush()
            if tail:
                pending += tail
            if pending:
                yield pending.decode("utf-8", errors="replace")

    def write_gfa(self, set_of_nodes=None,
                  output_file="output_file.gfa", append=True):
        """
        Write a gfa out
        :param self: the graph object
        :param set_of_nodes: A list of node ids of the path or nodes we want to generate a GFA file for.
        :param output_file: path to output file
        :param append: if I want to append to a file instead of rewriting it
        :return: writes a gfa file
        """

        if set_of_nodes is None:
            set_of_nodes = self.nodes.keys()

        if append is False:
            if output_file == "/dev/stdout" or output_file == "-":
                f = sys.stdout
            else:
                f = open(output_file, "w")
        else:
            if os.path.exists(output_file):
                f = open(output_file, "a")
            else:
                logging.warning("Trying to append to a non-existent file\n"
                                "creating an output file")
                f = open(output_file, "w")

        for n1 in set_of_nodes:
            if n1 not in self.nodes:
                logging.warning("Node {} does not exist in the graph, skipped in output".format(n1))
                continue

            line = self.nodes[n1].to_gfa_line()
            # line = str("\t".join(("S", str(n1), nodes[n1].seq, "LN:i:" + str(len(nodes[n1].seq)))))
            # if optional_info:
            # 	line += "\t" + nodes[n1].optional_info

            f.write(line + "\n")

            # writing edges
            # edges = []
            # overlap = str(graph.k - 1) + "M\n"

            for n in self.nodes[n1].start:
                overlap = str(n[2]) + "M\n"
                # I am checking if there are nodes I want to write
                # I think I can remove this later as I implemented the .remove_node
                # to the Graph class that safely removes a node and all its edges
                # So there shouldn't be any edges to removed
                if n[0] in set_of_nodes:
                    if n[1] == 0:
                        edge = str("\t".join(("L", str(n1), "-", str(n[0]), "+", overlap)))
                        # edge += "\n"
                    # edges.append(edge)
                    else:
                        edge = str("\t".join(("L", str(n1), "-", str(n[0]), "-", overlap)))
                        # edge += "\n"
                    # edges.append(edge)
                    f.write(edge)

            for n in self.nodes[n1].end:
                overlap = str(n[2]) + "M\n"

                if n[0] in set_of_nodes:
                    if n[1] == 0:
                        edge = str("\t".join(("L", str(n1), "+", str(n[0]), "+", overlap)))
                        # edge += "\n"
                    # edges.append(edge)
                    else:
                        edge = str("\t".join(("L", str(n1), "+", str(n[0]), "-", overlap)))
                        # edge += "\n"
                    # edges.append(edge)
                    f.write(edge)
        #
        # for e in edges:
        # 	f.write(e)

        if f is not sys.stdout:
            f.close()

    def path_exists(self, path):
        """
        Just a check that a path given exists in the graph
        I am assuming that the list of node given as ordered_path taken from the GAF alignment is ordered
        i.e., node 1 parent of node 2, node 2 parent of node 3 and so on
        """

        ordered_path = re.findall("[><][^><]+", path)
        cases = {
            (">", ">"): ("end", 0),
            ("<", "<"): ("start", 1),
            (">", "<"): ("end", 1),
            ("<", ">"): ("start", 0),
        }

        for i in range(1, len(ordered_path)):
            n1 = ordered_path[i - 1]
            n2 = ordered_path[i]
            try:
                case = cases[(n1[0], n2[0])]
            except KeyError:
                logging.error(
                    "Something went wrong when checking the path, make sure the path follows this format"
                    ">node<node>node<nod"
                )
                return False
            ok = False
            for edge in getattr(self[n1[1:]], case[0]):
                if (n2[1:], case[1]) == (edge[0], edge[1]):
                    ok = True
            if not ok:
                return False

        return True

    def extract_path_seq(self, path):
        """
        returns the sequences representing that path
        """
        seq = []
        # path has to start with > or <, otherwise it's invalid
        if path[0] not in {"<", ">"}:
            logging.error(f"The path {path} does not start with < or > ")
            return ""

        if not self.path_exists(path):
            logging.error(f"The path given {path} does not exist")
            return ""
        path = re.findall("[><][^><]+", path)

        for n in path:
            if n[1:] not in self:
                logging.error(f"The node {n[1:]} in path {path} does not seem to exist in this GFA")
                return ""
            # print(n)
            if n.startswith(">"):
                seq.append(self[n[1:]].seq)
            elif n.startswith("<"):
                seq.append(rev_comp(self[n[1:]].seq))
            # seq.append("".join([reverse_complement[x] for x in self.nodes[n[1:]].seq[::-1]]))
            else:
                logging.error(f"Some error happened where a node {n} doesn't start with > or <")
                return ""
        return "".join(seq)

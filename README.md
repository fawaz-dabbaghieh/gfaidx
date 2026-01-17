# gfaidx
A new implementation of my graph index, I wanted to start from scratch.
Now using a different Louvain Method implementation that uses much less memory.

# TODO
- [x] Add fast buffer-based GFA reader, inspired by `strangepg`
- [x] Generate edge lists from a GFA
    - [x] Integrate `strangepg` file reading for faster GFA loading
- [x] On disk binary search for node IDs to their community ID
    - [x] It works but needs to be implemented in the code to store the community IDs.
- [x] separate the GFA file based on the communities produced.
    - [x] Change the map to a string: <int, int> and the second Int is the community ID, or keep a vector of node length
          and add to it <string, int> with node id and community ID. Need to test memory for both.
- [x] Generate the community ID to file offset index (int: <int, int>, community ID: <start, end>)
    - [x] Need to look if I can then gzip the chunks separately and how will this change the offsets.
- [ ] Separate the edges that belong to different communities to their own chunk.
- [ ] As long as I'm hashing the sequence IDs later, maybe i should hash them first and use that sail dictionary that uses less memory.
- [ ] Parallelize the GFA chunking/gzipping.
- [ ] Maybe make the community index a binary file that gets loaded into memory completely for faster access.
- [x] Add command line interface
- [ ] Add unit tests
- [ ] Benchmark against other graph clustering tools
- [ ] Add Python interface
- [ ] Add Rust interface
- [ ] Add conda package

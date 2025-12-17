# gfaidx
A new implementation of my graph index, I wanted to start from scratch.
Now using a different Louvain Method implementation that uses much less memory.

# TODO
- [x] Add fast buffer based GFA reader, inspired by `strangepg`
- [x] Generate edge lists from a GFA
    - [ ] Integrate `strangepg` file reading for faster GFA loading
- [x] On disk binary search for node IDs to their community ID
    - [ ] It works but needs to be implemented in the code to store the community IDs.
- [ ] separate the GFA file based on the communities produced.
    - [ ] Change the map to a string: <int, int> and the second Int is the community ID, or keep a vector of node length
          and add to it <string, int> with node id and community ID. Need to test memory for both.
- [ ] Generate the community ID to file offset index (int: <int, int>, community ID: <start, end>)
    - [ ] Need to look if I can then gzip the chunks separately and how will this change the offsets.
- [ ] Add unit tests
- [ ] Add benchmarks
- [ ] Add Python interface
- [ ] Add Rust interface
- [ ] Add conda package

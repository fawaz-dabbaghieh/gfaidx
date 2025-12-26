import random
import sys

if len(sys.argv) < 3:
    print("You need to give an input gfa and output paths file")
    exit(1)

node_ids = set()
with open(sys.argv[1], 'r') as infile:
    for line in infile:
        if line.startswith('S'):
            parts = line.strip().split('\t')
            node_id = parts[1]
            node_ids.add(node_id)

node_ids = list(node_ids)

with open(sys.argv[2], 'w') as outfile:
    for i in range(200):  # number of paths
        outfile.write(f"P\trandom_path{i}\t")
        # write nodes
        for j in range(100000):
            outfile.write(random.choice(node_ids))
            outfile.write(random.choice(['+', '-']))
            if j != 100000 - 1:
                outfile.write(",")
        # overlaps
        outfile.write("\t*\n")

print("Done! Created 200 paths of 100,000 nodes each.")

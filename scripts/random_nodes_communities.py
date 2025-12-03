import os
import random
import string




communities = []
for i in range(1500):
    communities.append(i)

characters = string.ascii_letters + string.digits

with open('random_nodes_communities.txt', 'w') as outfile:
    for _ in range(50000):
        random_string = ''.join(random.choice(characters) for i in range(9))
        outfile.write(random_string + " " + str(random.choice(communities)) + "\n")
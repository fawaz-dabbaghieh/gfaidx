import random
import string
import sys


if len(sys.argv) < 4:
    print("You need to give the output file and the number of strings and the number of queries>")
    exit(1)

n_communities = int(sys.argv[2])
n_queries = int(sys.argv[3])

communities = []
for i in range(1500):
    communities.append(i)

queries = []
characters = string.ascii_letters + string.digits

choices = [True, False
           ]
with open(sys.argv[1], 'w') as outfile:
    for _ in range(n_communities):
        random_string = ''.join(random.choice(characters) for i in range(9))
        if random.choice(choices):
            queries.append(random_string)
            if len(queries) == n_queries:
                choices = [False]
        outfile.write(random_string + " " + str(random.choice(communities)) + "\n")
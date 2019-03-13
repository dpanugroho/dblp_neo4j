import pandas as pd
from random import randint

def prepare_author_affiliation():
    authors = pd.read_csv('out/authors.csv')
    universities = pd.read_csv('world_university.csv', header=None)[1]
    random_index = [randint(0, len(universities)-1) for i in range(len(authors))]
    authors_university = [universities[r] for r in random_index]
    authors['affiliation']=authors_university
    authors.to_csv('out/authors_with_affiliation.csv', index=None)

if __name__ == '__main__':
    prepare_author_affiliation()
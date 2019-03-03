#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 19:38:49 2019

@author: dpanugroho

CREDITS:
splitDataFrameList() -> https://gist.github.com/jlln/338b4b0b55bd6984f883

Spacy NER Label: https://spacy.io/api/annotation#named-entities
"""

import pandas as pd
import configparser
import spacy
from collections import Counter


config = configparser.ConfigParser()
config.read('conf.ini')

nlp = spacy.load('en')

scale = int(config['DATA']['scale'])
neo4j_import_dir = config['DATA']['neo4j_import_dir']



def splitDataFrameList(df,target_column,separator):
    ''' df = dataframe to split,
    target_column = the column containing the values to split
    separator = the symbol used to perform the split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row. 
    The values in the other columns are duplicated across the newly divided rows.
    '''
    row_accumulator = []

    def splitListToRows(row, separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)

    df.apply(splitListToRows, axis=1, args = (separator, ))
    new_df = pd.DataFrame(row_accumulator)
    return new_df

all_keyword = []
def extract_keyword_from_title(title):
    keywords = []
    for token in nlp(title):
        if token.pos_ == "NOUN":
            keywords.append(token.lower_)
    all_keyword.extend(keywords)
    return keywords

def extract_location(proceeding_title):
    locations = []
    for token in nlp(proceeding_title).ents:
        if token.label_ == "GPE":
            locations.append(token.text)
    return ",".join(locations)          

def get_corresponding_author(authors):
    return authors.split("|")[0]

articles = pd.read_csv('dblp_dump/output_article.csv',
                       delimiter=';',
                       usecols=['key','author','title','journal','volume',
                                'year','pages','mdate','ee'],
                       nrows=scale)
articles.dropna(inplace=True)
articles['corresponding_author'] = articles['author'].apply(get_corresponding_author)
article_authors = articles[['key','author']]
articles.drop('author', inplace=True, axis=1)
article_authors = splitDataFrameList(article_authors,'author','|')

journal = articles[['journal','volume']]
journal.drop_duplicates(subset=['journal','volume'], inplace=True)
inproceedings = pd.read_csv('dblp_dump/output_inproceedings.csv',
                    delimiter=';',
                    usecols=['key',
                             'author','title','booktitle','year','pages',
                             'mdate','ee'],
                    nrows=scale)
inproceedings.dropna(inplace=True)
inproceedings_authors = inproceedings[['key','author']]
inproceedings.drop('author', inplace=True, axis=1)

inproceedings_authors = splitDataFrameList(inproceedings_authors,'author','|')

proceedings = pd.read_csv('dblp_dump/output_proceedings.csv',
                          usecols=['key',
                                   'isbn','title','booktitle','editor',
                                   'publisher','series','mdate','ee',
                                   'volume','year'],
# for debug, proceedings should be fully loaded
#                                   nrows=scale,
                    delimiter=';')

# Proceeding Location
proceedings['Location'] = proceedings['title'].apply(extract_location)


# Randomly assign keyword
articles['keyword'] = articles['title'].apply(extract_keyword_from_title)
inproceedings['keyword'] = inproceedings['title'].apply(extract_keyword_from_title)

# Store keyword
all_keyword = Counter(all_keyword)
all_keyword = all_keyword.most_common(int(0.1*len(all_keyword)))
all_keyword = pd.DataFrame(all_keyword, columns=['keyword','occurence'])

# Grouping the authors
authors = article_authors.append(inproceedings_authors)
authors = authors.groupby('author')['key'].apply(list)
authors = authors.str.join('|')

# Export to CSV
articles.to_csv(neo4j_import_dir+'/articles.csv', index=None, header=True)
authors.to_csv(neo4j_import_dir+'/authors.csv', header=True)
proceedings.to_csv(neo4j_import_dir+'/proceedings.csv', index=None, header=True)
inproceedings.to_csv(neo4j_import_dir+'/inproceedings.csv', index=None, header=True)
all_keyword.to_csv(neo4j_import_dir+'/all_keyword.csv', index=None, header=True)



#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 19:38:49 2019

@author: dpanugroho

CREDITS:
splitDataFrameList() -> https://gist.github.com/jlln/338b4b0b55bd6984f883
"""

import pandas as pd



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


articles = pd.read_csv('dblp_dump/output_article.csv',
                       delimiter=';',
                       usecols=['key','author','title','journal','volume',
                                'year','pages','mdate','ee'],
                       nrows=10000)
articles.dropna(inplace=True)
article_authors = articles[['key','author']]
articles.drop('author', inplace=True, axis=1)
article_authors = splitDataFrameList(article_authors,'author','|')

inproceedings = pd.read_csv('dblp_dump/output_inproceedings.csv',
                       delimiter=';',
                       usecols=['key',
                               'author','title','booktitle','year','pages',
                                'mdate','ee'],
                       nrows=10000)
inproceedings.dropna(inplace=True)
inproceedings_authors = inproceedings[['key','author']]
inproceedings.drop('author', inplace=True, axis=1)
inproceedings_authors = splitDataFrameList(inproceedings_authors,'author','|')


proceedings = pd.read_csv('dblp_dump/output_proceedings.csv',
                          usecols=['key',
                                   'isbn','title','booktitle','editor',
                                    'publisher','series','mdate','ee',
                                    'volume','year'],
                       delimiter=';')

# Grouping the authors
authors = article_authors.append(inproceedings_authors)
authors = authors.groupby('author')['key'].apply(list)


# Export to CSV
articles.to_csv('neo4j_input/articles.csv', index=None, header=True)
authors.to_csv('neo4j_input/authors.csv', header=True)
proceedings.to_csv('neo4j_input/proceedings.csv', index=None, header=True)
inproceedings.to_csv('neo4j_input/inproceedings.csv', index=None, header=True)



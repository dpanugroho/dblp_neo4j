#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 17:41:03 2019

@author: dpanugroho
"""

import pandas as pd
import spacy
import random

from difflib import SequenceMatcher

def clean_citation_string(citation):
    if pd.notna(citation):
        citation = ([c for c in citation.split("|") if c != '...'])
    else:
        citation = []
    return "|".join(citation)

def get_cited_paper_key(citing_papers):
    cited_paper = []
    for citing_paper in citing_papers:
        cited_paper.extend(citing_paper.split('|'))
    cited_paper = [c for c in cited_paper if (c != '...' and 'book' not in c 
                                              and 'www' not in c 
                                              and 'phd' not in c)]
    return cited_paper

def add_corresponding_author(paper):
    corresponding_author = None
    try: 
        max_similarity = 0
        for candidate in paper['author']:
            current_distance = SequenceMatcher(None, candidate, 
                                               paper['key'].split('/')[-1]).ratio()
            if current_distance >= max_similarity:
                max_similarity = current_distance
                corresponding_author = candidate
    except AttributeError:
        pass
    return corresponding_author
      
def splitDataFrameList(df,target_column,separator):
    row_accumulator = []

    def splitListToRows(row, separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)
    try:
        df.apply(splitListToRows, axis=1, args = (separator, ))
    except AttributeError:
        pass
    
    new_df = pd.DataFrame(row_accumulator)
    return new_df

def extract_keyword_from_title(title):
    keywords = []
    for token in nlp(title):
        if token.pos_ == "NOUN":
            keywords.append(token.lower_)
    ALL_KEYWORDS.extend(keywords)
    return keywords

def get_experts(selected_paper):
    reviewer_candidates = selected_paper[['author','keyword']]
    reviewer_candidates = reviewer_candidates[reviewer_candidates.astype(str)['keyword'] != '[]']
    reviewer_candidates.dropna(inplace=True)
    res = []
    for _, row in reviewer_candidates.iterrows():
        for author in row['author']:
            for kw in row['keyword']:
                res.append((author, kw))
    res = pd.DataFrame(list(set(res)), columns=['name','domain'])
    res = res.groupby('domain')['name'].apply(list)
    return res

def assign_reviewer(keywords):
    # Caveat: There's still possibility of self-reviewing. This should be
    # filtered when loading the data to neo4j
    assigned_reviewer = []
    # If the paper has keyword, assign another author that has paper
    # in the same keyword
    if len(keywords)>0:
        for keyword in keywords:
            # Somehow it can't find some keys...
            try:
                assigned_reviewer.append(random.choice(experts[keyword]))
            except KeyError:
                pass
    # Otherwise, assign random author
    while len(assigned_reviewer)<3:
        assigned_reviewer.append(random.choice(authors.index))
    return assigned_reviewer
    
if __name__ == '__main__':
    nlp = spacy.load('en')
    ALL_KEYWORDS = []

    articles = pd.read_csv('dblp_dump/output_article.csv',
                           delimiter=';',
                           usecols=['key','author','title','year','ee', 'cite',
                                    'mdate','pages','journal','volume'])
    inproceedings = pd.read_csv('dblp_dump/output_inproceedings.csv',
                        delimiter=';',
                        usecols=['key','author','title','year','ee', 'cite',
                                 'mdate','pages','booktitle', 'crossref'])
    
    # Setting the type of paper
    articles['type'] = 'journal'
    inproceedings['type'] = 'conference'
    
    
    all_papers = pd.concat([articles,inproceedings], sort=False)
    
    articles.dropna(subset = ['cite'], inplace=True)
    inproceedings.dropna(subset = ['cite'], inplace=True)
    
    citing_papers = pd.concat([articles, inproceedings], sort=False)
    citing_paper_key = citing_papers['cite']
    cited_paper_key = get_cited_paper_key(citing_paper_key)
    
    cited_paper = all_papers[all_papers['key'].isin(cited_paper_key)]
    
    selected_paper = pd.concat([citing_papers,cited_paper], sort=False)
    selected_paper.drop_duplicates(inplace=True)
    selected_paper.dropna(subset=['author'], inplace=True)
    selected_paper['author']= selected_paper['author'].apply(lambda x: x.split('|'))
    
    selected_paper['corresponding_author'] = selected_paper.apply(add_corresponding_author, axis =1)
    
    # Clean citation string
    selected_paper['cite'] = selected_paper['cite'].apply(clean_citation_string)
    
    # Extract author and their list of paper
    authors = selected_paper[['author','key']]
    authors = authors.set_index(['key']).author.apply(pd.Series).stack().reset_index(name='author').drop('level_1', axis=1)
    authors = authors.groupby('author')['key'].apply(list).str.join('|')
    
    # Extract Keyword
    selected_paper['keyword'] = selected_paper['title'].apply(extract_keyword_from_title).str.join('|')    
   
    # Store keyword
    ALL_KEYWORDS = pd.DataFrame(ALL_KEYWORDS, columns=["keyword"]).drop_duplicates()
    experts = get_experts(selected_paper)
    
    # Assign reviewer to each paper
    selected_paper['reviewer'] = selected_paper['keyword'].apply(assign_reviewer).str.join('|')
    
    # Slice journal from articles and remove duplicates
    journals = articles[['journal','volume','year']]
    journals = journals.drop_duplicates()
    
    # Write outputs to csv
    selected_paper.to_csv('out/papers.csv', index=None, header=True)
    authors.to_csv('out/authors.csv', header=True)
    ALL_KEYWORDS.to_csv('out/keywords.csv', index=None, header=True)
    journals.to_csv('out/journals.csv', index=None, header=True)


#

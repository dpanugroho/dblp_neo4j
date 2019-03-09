#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 19:38:49 2019

@author: dpanugroho
Spacy NER Label: https://spacy.io/api/annotation#named-entities
"""

import pandas as pd
import spacy

nlp = spacy.load('en')

def extract_location(proceeding_title):
    locations = []
    for token in nlp(proceeding_title).ents:
        if token.label_ == "GPE":
            locations.append(token.text)
    return ",".join(locations)          

proceedings = pd.read_csv('dblp_dump/output_proceedings.csv',
                          usecols=['key',
                                   'isbn','title','booktitle','editor',
                                   'publisher','series','mdate','ee',
                                   'volume','year'],

                            delimiter=';')

# Extract conference locationl
proceedings['location'] = proceedings['title'].apply(extract_location)

# Export to CSV
proceedings.to_csv('out/proceedings.csv', index=None, header=True)



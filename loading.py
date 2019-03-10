#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 18:36:45 2019

@author: dpanugroho

Script to load csv to neo4j
"""

from neo4j import GraphDatabase, basic_auth
import configparser

config = configparser.ConfigParser()
config.read('conf.ini')

ip = config['SERVER']['ip']


driver = GraphDatabase.driver('bolt://'+ip+':7687',
                              auth=basic_auth("neo4j", "neo4j"))

def create_paper_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
        AS row
        CREATE (paper:Paper 
        {ee:row.ee, journal:row.journal, key:row.key, mdate:row.mdate, 
        pages:row.pages, title:row.title, type:row.type, cite:row.cite, crossref:row.crossref, 
        reviewer:row.reviewer, keyword:row.keyword});
        
    """
    session.run(query)
def create_author_node(session):
    query = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM "file:///authors.csv" 
        AS row
        CREATE (:Author {name:row.author, key:row.key})
    """
    session.run(query)
    
def create_proceeding_node(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///proceedings.csv' AS row
    CREATE (:Proceeding {series:row.series, location:row.location,
    mdate:row.mdate,year:row.year,key:row.key,editor:row.editor,
    publisher:row.publisher,isbn:row.isbn,booktitle:row.booktitle,
    title:row.title,ee:row.ee,volume:row.volume})
    """    
    session.run(query)
    
def create_index_on_paper_key(session):
    query = "CREATE INDEX ON :Paper(key)"
    session.run(query)
    
def drop_index_on_paper_key(session):
    query = "DROP INDEX ON :Paper(key)"
    session.run(query)

def create_index_on_author_name(session):
    query = "CREATE INDEX ON :Author(name)"
    session.run(query)

def drop_index_on_author_name(session):
    query = "DROP INDEX ON :Author(name)"
    session.run(query)

def create_index_on_keyword(session):
    query = "CREATE INDEX ON :Keyword(keyword)"
    session.run(query)

def drop_index_on_keyword(session):
    query = "DROP INDEX ON :Keyword(keyword)"
    session.run(query)

def create_index_on_journal(session):
    query = "CREATE INDEX ON :Journal(name)"
    session.run(query)
    
def drop_index_on_journal(session):
    query = "DROP INDEX ON :Journal(name)"
    session.run(query)

def create_index_on_proceeding(session):
    query = "CREATE INDEX ON :Proceeding(key)"
    session.run(query)
    
def drop_index_on_proceeding(session):
    query = "DROP INDEX ON :Proceeding(key)"
    session.run(query)


def create_write_relation(session): 
    query = """
        MATCH (author:Author) 
        UNWIND SPLIT(author.key,"|") as key
        MATCH (paper:Paper {key:key})
        MERGE (author)-[:Write]->(paper);
    """
    session.run(query)

def create_keyword_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///keywords.csv' 
        AS row
        CREATE (:Keyword {keyword:row.keyword})
    """
    session.run(query)

def create_journal_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///journals.csv' 
        AS row
        CREATE (:Journal {name:row.journal, volume:row.volume, year:row.year})
    """
    session.run(query)
    
def create_reviewedby_relation(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
        AS row
        MATCH (paper:Paper {key:row.key})
        UNWIND SPLIT(row.reviewer,"|") as reviewer
        MATCH (author:Author {name:reviewer})
        MERGE (paper)-[:ReviewedBy]->(author)
    """
    session.run(query)
    
def create_publishedin_journal_relation(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
    AS row
    MATCH (journal:Journal {name:row.journal, year:row.year,volume:row.volume})
    MATCH (paper:Paper {key:row.key})
    MERGE (paper)-[:PublishedIn]->(journal)
    """
    session.run(query)
    
def create_published_in_proceeding_relation(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
        AS row
        MATCH (proceeding:Proceeding {key:row.crossref})
        MATCH (paper:Paper {key:row.key})
        MERGE (paper)-[:PublishedIn]->(proceeding)
    """
    session.run(query)
    
def create_cite_relation(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
    AS row
    UNWIND SPLIT(row.cite,"|") as citedPaperKey
    MATCH (citingPaper:Paper {key:row.key})
    MATCH (citedPaper:Paper {key:citedPaperKey})
    MERGE (citingPaper)-[:Cite]->(citedPaper)
    """
    session.run(query)

def create_has_keyword_relation(session):
    """
    LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
    AS row
    UNWIND SPLIT(row.keyword,"|") as keyword
    MATCH (paper:Paper {key:row.key})
    MATCH (usedKeyword:Keyword {keyword:keyword})
    MERGE (paper)-[:HasKeyword]-(usedKeyword)
    """

def delete_all_relation(session): 
    query = "MATCH ()-[r]-() DELETE r"
    session.run(query)
def delete_all_node(session): 
    query = "MATCH (n) DELETE n"
    session.run(query)

with driver.session() as session:
    delete_all_relation(session)
    delete_all_node(session)    
    
#     Optional, if index exists
    drop_index_on_paper_key(session)
    drop_index_on_author_name(session)
    drop_index_on_keyword(session)
    
    create_proceeding_node(session)
    create_journal_node(session)
    create_keyword_node(session)
    create_index_on_journal(session)

    create_paper_node(session)
    create_author_node(session)
    
    create_index_on_paper_key(session)
    create_index_on_author_name(session)
    create_index_on_keyword(session)
    create_index_on_proceeding(session)

    create_write_relation(session)
    create_reviewedby_relation(session)
    create_publishedin_journal_relation(session)
    create_cite_relation(session)
    create_has_keyword_relation(session)
    create_published_in_proceeding_relation(session)
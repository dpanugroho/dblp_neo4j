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

create_journal_paper_node_query= """USING PERIODIC COMMIT
                           LOAD CSV WITH HEADERS FROM 'file:///articles.csv' 
                           AS row
                           CREATE (:Paper 
                               {ee:row.ee, journal:row.journal, 
                                key:row.key, mdate:row.mdate, 
                                pages:row.pages, title:row.title, 
                                volume:row.volume,year:row.year, 
                                src:'articles'});"""
create_inproceeding_paper_node_query = """USING PERIODIC COMMIT
                                        LOAD CSV WITH HEADERS 
                                        FROM "file:///inproceedings.csv" AS row
                                        CREATE (:Paper 
                                        {booktitle:row.booktitle,
                                         ee:row.ee,key:row.key,mdate:row.mdate,
                                         pages:row.pages, title:row.title, 
                                         year:row.year, 
                                         src:"inproceedings"});"""
create_author_node_query = """USING PERIODIC COMMIT
                            LOAD CSV WITH HEADERS FROM "file:///authors.csv" 
                            AS row
                            CREATE (:Author {name:row.author, key:row.key})"""

create_index_on_paper_key = "CREATE INDEX ON :Paper(key)"
drop_index_on_paper_key = "DROP INDEX ON :Paper(key)"

create_index_on_author_name = "CREATE INDEX ON :Author(name)"
drop_index_on_author_name = "DROP INDEX ON :Author(name)"

create_write_relation = """MATCH (author:Author) 
                        UNWIND author.key as key
                        MATCH (paper:Paper {key:key})
                        MERGE (author)-[:Write]->(paper);"""

delete_all_relation = "MATCH ()-[r]-() DELETE r"
delete_all_node = "MATCH (n) DELETE n"

with driver.session() as session:
    session.run(delete_all_relation)
    session.run(delete_all_node)
    
    # Optional, if index exists
    # session.run(drop_index_on_paper_key)
    # session.run(drop_index_on_author_name)
    
    session.run(create_journal_paper_node_query)
    session.run(create_inproceeding_paper_node_query)
    session.run(create_author_node_query)
    session.run(create_index_on_paper_key)
    session.run(create_index_on_author_name)
    session.run(create_write_relation)
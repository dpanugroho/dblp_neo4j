#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 18:36:45 2019

@author: dpanugroho

Script to run reviewer recommender
"""
from neo4j import GraphDatabase, basic_auth
import configparser
import pandas as pd
import optparse

def article_rank(session):
    query = """
    CALL algo.articleRank.stream('Paper', 'Cite', {iterations:20, dampingFactor:0.85})
    YIELD nodeId, score
    RETURN algo.getNodeById(nodeId).title AS paper,score
    ORDER BY score DESC
    """
    return pd.DataFrame([(record['paper'], record['score']) for record in session.run(query)], columns=['paper','score'])

def coauthor_community(session):
    query = """
    CALL algo.louvain.stream('Author', 'CoauthorWith', {})
    YIELD nodeId, community
    RETURN community, COLLECT(algo.getNodeById(nodeId).name) AS authorList
    """
    return pd.DataFrame([(record['community'], record['authorList']) for record in session.run(query)], columns=['community','authorList'])


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('conf.ini')

    # Parse user's option
    parser = optparse.OptionParser()
    parser.add_option("-a", "--algorithmnum", help="algorithm number to execute, 1=articlerank, 2=community detection", type="int")

    (options, args) = parser.parse_args()

    if not (options.algorithmnum):
        parser.print_help()
        exit(1)


    ip = config['SERVER']['ip']

    driver = GraphDatabase.driver('bolt://'+ip+':7687',
                              auth=basic_auth("neo4j", "neo4j"))

    with driver.session() as session: 
        if options.algorithmnum==1:
            print(article_rank(session))
        elif options.algorithmnum==2:
            print(coauthor_community(session))
        else:
            print("Please select the number of 1 to 2")


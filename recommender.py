#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 18:36:45 2019

@author: dpanugroho

Script to run reviewer recommender
"""
from neo4j import GraphDatabase, basic_auth
import configparser

def get_community_conference(session, community_threshold, keyword_list):
    """Function to get list of conference regarded as community.

    Args:
        session: Neo4J session.
        community_threshold: Minimum percentage of similarity to define that a conference is a community
        keyword_list: List of keyword of to define the community
    Returns:
        List of community
    """
    query = """
    MATCH (dbKeyword:Keyword)<-[:HasKeyword]-(paperAboutKeywords:Paper)-[:PublishedIn]->(publication)-[:PartOf]->(book:Book)
    WHERE dbKeyword.keyword in """+str(keyword_list)+"""
    WITH book, COUNT(DISTINCT paperAboutKeywords) AS nPaperAboutKeywords 
    MATCH (book)<-[:PartOf]-(publication)<-[:PublishedIn]-(paperInBook:Paper)
    WITH book,nPaperAboutKeywords,COUNT(paperInBook) as nPaperInBook
    WITH book,nPaperAboutKeywords,nPaperInBook,(tofloat(nPaperAboutKeywords)/nPaperInBook) as percentagepaperAboutKeywords 
    WHERE percentagepaperAboutKeywords>="""+str(community_threshold)+"""
    WITH book.title as booktitle, nPaperAboutKeywords, nPaperInBook, percentagepaperAboutKeywords
    order by percentagepaperAboutKeywords desc
    RETURN collect(booktitle)
    """
    return session.run(query).single()[0]

# STEP 2: Get (at most) top 100 paper with highest page rank
def get_top_paper_in_conference(session, conferences):
    """Function to get list of top paper in the community.

    Args:
        session: Neo4J session.
        conferences: List of conference title regarded as a community

    Returns:
        List of top papers in the community
    """
    query = """
    CALL algo.pageRank.stream('Paper', 'Cite', {iterations:20, dampingFactor:0.85})
    YIELD nodeId, score
    WITH algo.getNodeById(nodeId) AS paper,score
    MATCH (book:Book)<-[:PartOf]-(:Proceeding)<-[:PublishedIn]-(paper)
    WHERE book.title IN """+str(conferences)+""" //these values should comes from STEP 1
    WITH paper.title as papertitle, book.title as booktitle, score     ORDER BY score DESC
    RETURN COLLECT(papertitle)
    LIMIT 100
    """
    return session.run(query).single()[0]


# STEP 3: Define Gurus 
def get_gurus(session, top_papers, min_paper):
    """Function to get list of top paper in the community.

    Args:
        session: Neo4J session.
        top_papers: List of top papers in the community

    Returns:
        List of top papers in the community
    """
    query = """
    MATCH (paper:Paper)<-[:Write]-(author:Author)
    // the list should be from STEP 2
    WHERE paper.title IN """+str(top_papers)+"""
    WITH author.name as author, COUNT(paper) AS paperCount
    WHERE paperCount>"""+str(min_paper)+"""
    RETURN COLLECT(author)
    """
    return session.run(query).single()[0]


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('conf.ini')

    ip = config['SERVER']['ip']

    driver = GraphDatabase.driver('bolt://'+ip+':7687',
                              auth=basic_auth("neo4j", "neo4j"))

    with driver.session() as session: 
        db_keywords = ["database","index","indexing","querying","data","sql","olap","b+-trees","dbms","dbmss","views","databases","queries","postgres","picodbms","xml","tables","oodbms","relational"]
        db_conferences = get_community_conference(session,0.5,db_keywords)
        top_db_papers = get_top_paper_in_conference(session, db_conferences)
        
        # Currently, no one in this dataset published more than 1 top papers in db_conferences
        db_gurus = get_gurus(session,top_db_papers,0)
        print(db_gurus)




#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 18:36:45 2019

@author: dpanugroho

Script to run reviewer recommender
"""
from neo4j import GraphDatabase, basic_auth
import configparser
import optparse
import ast
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
    WHERE paperCount>="""+str(min_paper)+"""
    RETURN COLLECT(author)
    """
    return session.run(query).single()[0]

def execute_pipeline(minpaper, threshold, keywords, session):
    db_conferences = get_community_conference(session,threshold,keywords)
    top_db_papers = get_top_paper_in_conference(session, db_conferences)
    db_gurus = get_gurus(session,top_db_papers,minpaper)

    return db_gurus

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('conf.ini')

    ip = config['SERVER']['ip']

    # Parse user's option
    parser = optparse.OptionParser()
    parser.add_option("-m", "--minpaper", help="minimum paper appears in top 100 to be considered as guru", type="int")
    parser.add_option("-t", "--threshold", help="(fraction) homogenity threshold for a conference to be considered as community", type="float")
    parser.add_option("-k", "--keywords", help="""string representation of keywords, e.g:'["database", "sql"]' """, type="string")

    (options, args) = parser.parse_args()

    if not (options.minpaper and options.threshold and options.keywords):
        parser.print_help()
        exit(1)
    driver = GraphDatabase.driver('bolt://'+ip+':7687',
                              auth=basic_auth("neo4j", "neo4j"))

    parsed_keywords = ast.literal_eval(options.keywords)
    with driver.session() as session: 
        gurus = execute_pipeline(options.minpaper,options.threshold,parsed_keywords, session)
        print(gurus)




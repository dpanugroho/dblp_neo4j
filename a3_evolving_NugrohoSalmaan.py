#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 18:36:45 2019

@author: dpanugroho

Script to run reviewer recommender
"""
from neo4j import GraphDatabase, basic_auth
import configparser


def assignSuggestedDecision(session):
    query = """
        MATCH (p:Paper)-[r:ReviewedBy]-(a:Author)
        WITH r,(rand()+0.3) AS acceptanceProbability
        WITH r, acceptanceProbability,
        CASE
        WHEN acceptanceProbability>0.5 THEN True
        ELSE False END AS suggestedDecision
        SET r.suggestedDecision=suggestedDecision
    """
    session.run(query)

def  delete_rejected_paper(session):
    query = """
    MATCH (:Author)<-[negativeReview:ReviewedBy {suggestedDecision:false}]-(paper:Paper)-[positiveReview:ReviewedBy {suggestedDecision:true}]->(:Author) 
    WITH paper,negativeReview, COUNT(positiveReview) AS positiveReviewCount 
    WITH paper, positiveReviewCount, COUNT(negativeReview) AS negativeReviewCount
    WHERE negativeReviewCount>positiveReviewCount
    DETACH DELETE paper
    """
    session.run(query)

def assign_author_affiliation(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///authors_with_affiliation.csv' AS ROW
    MATCH (a:Author {name:ROW.author})
    SET a.affiliation = ROW.affiliation
    """
    session.run(query)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('conf.ini')

    ip = config['SERVER']['ip']

    driver = GraphDatabase.driver('bolt://'+ip+':7687',
                              auth=basic_auth("neo4j", "neo4j"))

    with driver.session() as session: 
        assign_author_affiliation(session)
        assignSuggestedDecision(session)
        delete_rejected_paper(session)





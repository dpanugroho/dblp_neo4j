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
def h_index(session):
    query = """
        match (a:Author)-[w:Write]->(p:Paper)-[c:Cite]->(p1:Paper)
        with a,p1,collect([id(p),p.title]) as rows
        WITH a,p1,RANGE(1,SIZE(rows))AS enumerated_rows
        UNWIND enumerated_rows AS er
        with a,er AS rank,count(p1) as cited
        where rank <= cited 
        return a.name as authorname, rank as hindex
        order by hindex desc
    """
    return pd.DataFrame([(record['authorname'], record['hindex']) for record in session.run(query)], columns=['authorname','hindex'])

def top_three_most_cited(session):
    query = """
        MATCH (citingPaper:Paper)-[citation:Cite]->(citedPaper:Paper)-[:PublishedIn]-(proceeding:Proceeding)
        WITH COUNT(citation) AS citedCount, citedPaper, proceeding ORDER BY citedCount DESC
        WITH COLLECT(citedPaper.title) AS mostCitedPaper, proceeding
        RETURN proceeding.title as conference, mostCitedPaper[0..3] as mostCitedPapers
    """
    return pd.DataFrame([(record['conference'], record['mostCitedPapers']) for record in session.run(query)], columns=['conference','top_3_papers'])

def published_four_different_edition(session):
    query = """
        MATCH (a:Author)-[:Write]->(pa:Paper)-[pi:PublishedIn]->(p:Proceeding)
        with a, p, count(pa.key) as no_of_edition
        where p.key=~ 'conf.*'
        and no_of_edition>=4
        return a.name as authorname,p.key as conf_name,no_of_edition
    """
    return pd.DataFrame([(record['authorname'], record['conf_name'], record['no_of_edition']) for record in session.run(query)], columns=['authorname','conf_name','no_of_edition'])

def impact_factor(session, year):
    query = """
        MATCH (p1:Paper)-[:PublishedIn]->(j1:Journal)
        WHERE j1.year= """+str(year-1)+""" OR j1.year= """+str(year-2)+"""
        WITH j1.name as JournalName, size(COLLECT(p1)) AS nop, COLLECT(p1) AS c_journal
        MATCH(p1:Paper)-[c1:Cite]->(p2:Paper)
        WHERE p1 IN c_journal
        RETURN JournalName, (toFloat(COUNT(c1))/nop) AS ImpactFactor ORDER BY ImpactFactor DESC
    """
    return pd.DataFrame([(record['JournalName'], record['ImpactFactor']) for record in session.run(query)], columns=['JournalName','ImpactFactor'])



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('conf.ini')

    ip = config['SERVER']['ip']

    driver = GraphDatabase.driver('bolt://'+ip+':7687',
                              auth=basic_auth("neo4j", "neo4j"))

    with driver.session() as session: 
        print(h_index(session))
        print(top_three_most_cited(session))
        print(published_four_different_edition(session))
        print(impact_factor(session, 1999))

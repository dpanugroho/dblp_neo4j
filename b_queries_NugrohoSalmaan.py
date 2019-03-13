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

    # Parse user's option
    parser = optparse.OptionParser()
    parser.add_option("-q", "--querynum", help="query number to execute", type="int")
    parser.add_option("-y", "--year", help="the year for which the impact factor calculated", type="int")

    (options, args) = parser.parse_args()

    if not (options.querynum):
        parser.print_help()
        exit(1)

    ip = config['SERVER']['ip']

    driver = GraphDatabase.driver('bolt://'+ip+':7687',
                              auth=basic_auth("neo4j", "neo4j"))

    with driver.session() as session: 
        if options.querynum==1:
            print(h_index(session))
        elif options.querynum==2:
            print(top_three_most_cited(session))
        elif options.querynum==3:
            print(published_four_different_edition(session))
        elif options.querynum==4:
            if not (options.year):
                print("You have to specify the year option to run query 4 (using -y)")
                exit(1)
            print(impact_factor(session, int(options.year)))

        else:
            print("Please select the number of 1 to 4")

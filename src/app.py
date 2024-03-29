#!/usr/bin/env python3

import csv
import logging

from dataclasses import dataclass
from collections import defaultdict
from datetime import date

from SPARQLWrapper import SPARQLWrapper, CSV
from sqlalchemy import create_engine, Engine, Table, Column, select, insert, update
from sqlalchemy.orm import Session

from main.models import City, School, Dzi, Score
from main.import_utils import (
    get_existing_objects,
    prepare_for_insert_or_update,
    insert_objects,
    update_objects
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SparqlRepo:
    endpoint: str
    uri_prefix: str

    def get_csv_results(self, query: str):
        sparql = SPARQLWrapper(self.endpoint)

        sparql.setQuery(query)
        sparql.setReturnFormat(CSV)
        csv_blob = sparql.query().convert()
        csv_blob = csv_blob.decode('utf-8')
        csv_blob = csv_blob.split('\r\n')

        result = []
        for row in csv.reader(csv_blob):
            for i, cell in enumerate(row):
                row[i] = self.uri_to_id(cell)
            result.append(row)

        return result[1:]

    def uri_to_id(self, value: str):
        # This is hackish way to convert https://schools.ontotext.com/data/resource/<type>/<id>
        # to <type>:<id>
        # TODO: What's the proper way?
        if value and value.startswith(self.uri_prefix):
            r = value.replace(self.uri_prefix, '')
            r = r.replace('/', ':')
            return r
        else:
            return value


SCHOOLS_REPO = SparqlRepo(
    endpoint='https://schools.ontotext.com/data/repositories/schools',
    uri_prefix='https://schools.ontotext.com/data/resource/'
)


SPARQL_CITY = '''
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
SELECT distinct ?city ?cityLabel
 WHERE {
    ?city rdf:type schema:City;
            rdfs:label ?cityLabel.

    FILTER(lang(?cityLabel)="bg")
}
'''

SPARQL_SCHOOL = '''
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
SELECT distinct ?school ?schoolName ?schoolPlace
 WHERE {
    ?school rdf:type schema:School;
            schema:name ?schoolName;
            :place ?schoolPlace.

    ?schoolPlace rdf:type schema:City.

    FILTER(lang(?schoolName)="bg")
}
ORDER BY ?school ?schoolPlace
'''

SPARQL_DZI = '''
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX cube: <https://schools.ontotext.com/data/resource/cube/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT distinct ?dzi ?dziDate ?dziLabel ?dziComment
 WHERE {
    ?dzi qb:structure cube:dzi;
         :date ?dziDate;
         rdfs:label ?dziLabel;
         rdfs:comment ?dziComment.
}
ORDER by ?dzi
'''

SPARQL_SCORE = '''
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX cube: <https://schools.ontotext.com/data/resource/cube/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT distinct ?dziScore ?dzi ?school ?subject ?evalScore ?grade6 ?gradeLevel ?quantityPeople
 WHERE {
    ?dzi qb:structure cube:dzi.

    ?dziScore qb:dataSet ?dzi;
              rdf:type qb:Observation;
              :subject ?subject;
              :eval_score ?evalScore;
              :grade_6 ?grade6;
              :grade_level ?gradeLevel;
              :quantity_people ?quantityPeople;
              :rank_percentile ?rankPercentile;
              :school ?school.
}
order by ?dzi ?school ?subject
'''


def import_sparql_query(db, sparql_repo: SparqlRepo, query: str, table: Table, id_column: Column, raw_data_processor = None):

    logger.info(f'Executing SPARQL query for {table.name}')
    raw_objects = sparql_repo.get_csv_results(query)
    logger.debug(f'Executing SPARQL query for {table.name}')

    if raw_data_processor:
        logger.debug(f'Executing raw data processor for {table.name}')
        raw_objects = raw_data_processor(raw_objects)

    with Session(db) as session:
        existing_ids = get_existing_objects(session, id_column)

        to_insert, to_update = prepare_for_insert_or_update(
            table, raw_objects, existing_ids
        )

        logger.debug(f'Inserting new objects for {table.name}')
        insert_objects(session, table, to_insert)
        logger.debug(f'Updataing existing objects for {table.name}')
        update_objects(session, table, to_update)

        session.commit()


def drop_duplicated_schools(schools):
    schools_by_id = defaultdict(list)
    for s in schools:
        if len(s) == 3:
            schools_by_id[s[0]].append(s)

    # add all schools without duplicates
    result = [
        schools[0]
        for schools in schools_by_id.values()
        if len(schools) == 1
    ]

    dups = {
        id : schools
        for id, schools in schools_by_id.items()
        if len(schools) > 1
    }
    for schools in dups.values():
        result.append(schools[0])
        logger.info(f'school with duplicated cities (:place predicate): {schools[0]}')
        for dup_school in schools[1:]:
            logger.debug(f'    duplicate: {dup_school}')

    return result


def convert_dzi_date(raw_objects):
    for dzi_item in raw_objects:
        if len(dzi_item) == 4:
            dzi_item[1] = date.fromisoformat(dzi_item[1])
    return raw_objects


def drop_score_duplicates(raw_objects):
    col_count = len(Score.c)
    d = defaultdict(list)
    for score_item in raw_objects:
        if len(score_item) == col_count:
            d[score_item[0]].append(score_item)

    # add all items without duplicates
    result = [
        scores[0]
        for scores in d.values()
        if len(scores) == 1
    ]

    dups = {
        id: scores
        for id, scores in d.items()
        if len(scores) > 1
    }

    for id, scores in dups.items():
        logger.info(f'Dropping score {id} because it has duplicates')
        for dup in scores:
            logger.debug(f'    {dup}')

    return result

def convert_score_types(raw_objects):
    raw_objects = drop_score_duplicates(raw_objects)

    col_count = len(Score.c)
    for score_item in raw_objects:
        # ?dziScore ?dzi ?school ?subject ?evalScore ?grade6 ?gradeLevel ?quantityPeople
        if len(score_item) == col_count:
            score_item[4] = float(score_item[4]) # evalScore
            score_item[5] = float(score_item[5]) # grade6
            score_item[6] = int(score_item[6]) # grageLevel
            score_item[7] = float(score_item[7]) # quantityPeople

    return raw_objects


def main():
    # TODO: properly specify path to the sqlite db
    db_url = 'sqlite:////home/vitali/projects/data-for-good/educational-data/data/data.sqlite'
    db = create_engine(db_url)

    # TODO: call alembic programatically to create/update db schema

    import_sparql_query(db, SCHOOLS_REPO, SPARQL_CITY, City, City.c.id, None)
    import_sparql_query(db, SCHOOLS_REPO, SPARQL_SCHOOL, School, School.c.id, drop_duplicated_schools)
    import_sparql_query(db, SCHOOLS_REPO, SPARQL_DZI, Dzi, Dzi.c.id, convert_dzi_date)
    import_sparql_query(db, SCHOOLS_REPO, SPARQL_SCORE, Score, Score.c.id, convert_score_types)


if __name__ == '__main__':
    main()

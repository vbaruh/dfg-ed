#!/usr/bin/env python3

import csv
import logging

from dataclasses import dataclass
from collections import defaultdict
from datetime import date

from SPARQLWrapper import SPARQLWrapper, CSV
from sqlalchemy import create_engine, Engine, Table, Column, select, insert, update
from sqlalchemy.orm import Session

from main.models import City, School, Dzi
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


def import_cities(db: Engine):
    cities_query = '''
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
    cities = SCHOOLS_REPO.get_csv_results(cities_query)
    with Session(db) as session:

        existing_cities = get_existing_objects(session, City.c.id)

        to_insert, to_update = prepare_for_insert_or_update(
            City, cities, existing_cities
        )
        insert_objects(session, City, to_insert)
        update_objects(session, City, to_update)

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
        print(f'school with duplicates: {schools[0]}')
        for dup_school in schools[1:]:
            print(f'    duplicate: {dup_school}')

    return result


def import_schools(db):
    query = '''
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
    schools = SCHOOLS_REPO.get_csv_results(query)

    schools = drop_duplicated_schools(schools)

    with Session(db) as session:

        existing_schools = get_existing_objects(session, School.c.id)

        to_insert, to_update = prepare_for_insert_or_update(
            School, schools, existing_schools
        )
        insert_objects(session, School, to_insert)
        update_objects(session, School, to_update)

        session.commit()


def import_dzi(db):
    query = '''
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
    dzi = SCHOOLS_REPO.get_csv_results(query)
    # convert from str to date
    for dzi_item in dzi:
        if len(dzi_item) == 4:
            dzi_item[1] = date.fromisoformat(dzi_item[1])

    with Session(db) as session:
        existing_dzi = get_existing_objects(session, Dzi.c.id)

        to_insert, to_update = prepare_for_insert_or_update(
            Dzi, dzi, existing_dzi
        )

        insert_objects(session, Dzi, to_insert)
        update_objects(session, Dzi, to_update)

        session.commit()


def main():
    db_url = 'sqlite:////home/vitali/projects/data-for-good/educational-data/data/data.sqlite'
    db = create_engine(db_url)
    import_cities(db)
    import_schools(db)
    import_dzi(db)


if __name__ == '__main__':
    main()

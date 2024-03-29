#!/usr/bin/env python3

import csv
from dataclasses import dataclass
from collections import defaultdict

from SPARQLWrapper import SPARQLWrapper, CSV
from sqlalchemy import create_engine, Engine, Table, select, insert, update
from sqlalchemy.orm import Session

from main.models import City, School

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


def get_existing_cities(session: Session):
    existing_cities = set()
    st = select(City.c.id).distinct()
    print(st)

    for row in session.execute(st):
        existing_cities.add(row[0])

    return existing_cities


def insert_cities(cities, session: Session):
    for city in cities:
        session.execute((
            insert(City)
            .values(id=city[0], label=city[1])
        ))


def update_cities(cities, session: Session):
    for city in cities:
        session.execute((
            update(City)
            .where(City.c.id == city[0])
            .values(label=city[1])
        ))


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

        existing_cities = get_existing_cities(session)

        to_insert = []
        to_update = []

        for city_row in cities:
            if len(city_row) == 2:
                if city_row[0] in existing_cities:
                    to_update.append(city_row)
                else:
                    to_insert.append(city_row)

        print(f'cities to insert: {len(to_insert)}')
        print(f'cities to update: {len(to_update)}')

        insert_cities(to_insert, session)
        update_cities(to_update, session)

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


def get_existing_schools(session: Session):
    existing_schools = set()

    for row in session.execute(select(School.c.id).distinct()):
        existing_schools.add(row[0])

    return existing_schools


def insert_schools(schools, session: Session):
    for school in schools:
        session.execute((
            insert(School)
            .values(id=school[0], name=school[1], city_id=school[2])
        ))


def update_schools(schools, session: Session):
    for school in schools:
        session.execute((
            update(School)
            .where(School.c.id == school[0])
            .values(name=school[1],city_id=school[2])
        ))


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

        existing_schools = get_existing_schools(session)

        to_insert = []
        to_update = []

        for school_row in schools:
            if len(school_row) == 3:
                if school_row[0] in existing_schools:
                    to_update.append(school_row)
                else:
                    to_insert.append(school_row)

        print(f'schools to insert: {len(to_insert)}')
        print(f'schools to update: {len(to_update)}')

        insert_schools(to_insert, session)
        update_schools(to_update, session)

        session.commit()

def main():
    db_url = 'sqlite:////home/vitali/projects/data-for-good/educational-data/data/data.sqlite'
    db = create_engine(db_url)
    import_cities(db)
    import_schools(db)


if __name__ == '__main__':
    main()

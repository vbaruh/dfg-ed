from SPARQLWrapper import SPARQLWrapper, CSV


def main():
    sparql = SPARQLWrapper('https://schools.ontotext.com/data/repositories/schools')

    query = """
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
BASE <https://schools.ontotext.com/data/resource/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX schema: <http://schema.org/>
SELECT ?province ?town ?municipality ?school_wiki_id ?school ?year ?sub_group (SUM(?num_pup) AS ?tot_pup) (SUM(?grade*?num_pup)/?tot_pup AS ?average_grade)
  WHERE {
        ?s rdf:type qb:Observation;
           :date ?date;
           :grade_6 ?grade;
           :quantity_people ?num_pup;
           :school ?sch;
           :subject ?subject;
           :grade_level 12 .
        ?sch schema:name ?school;
             :wikidata_entity ?school_wiki_id;
             :place ?place.
        ?subject skos:prefLabel ?sub_abbr.
        ?place rdfs:label ?town;
                geo:sfWithin ?mun.
        ?mun geo:sfWithin ?prov;
             rdfs:label ?municipality.
        ?prov rdfs:label ?province.
        FILTER(lang(?town)="bg")
        FILTER(lang(?province)="bg")
    	FILTER(lang(?municipality)="bg")
        BIND (year(?date) as ?year)
        BIND (if (?sub_abbr in ("Математика"@bg, "Биология и здравно образование"@bg, "Физика и астрономия"@bg, "Химия и опазване на околната среда"@bg), "СТЕМ"@bg, if (?sub_abbr in ("Български език и литература"@bg), "БЕЛ"@bg, "ДРУГИ"@bg)) as ?sub_group)
} GROUP BY ?province ?town ?municipality ?school_wiki_id ?school ?year ?sub_group
ORDER BY ?province ?town ?school_wiki_id ?school ?year
"""
    sparql.setQuery(query)
    sparql.setReturnFormat(CSV)   # Return format is JSON
    results = sparql.query().convert()
    breakpoint()


if __name__ == '__main__':
    main()
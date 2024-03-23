# Working queries


## List of schools and their place

```
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
SELECT ?schoolName ?schoolPlace
 WHERE {
    ?school rdf:type schema:School;
            schema:name ?schoolName;
            :place ?schoolPlace.
}
```


## List of Municipalities

```
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?place ?placeName
WHERE {
    ?place rdf:type schema:Place;
           rdf:type schema:AdministrativeArea;
           rdf:type :Municipality;
           rdfs:label ?placeName.
    FILTER(lang(?placeName)="bg")
}
```

# list of schools

PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX place: <https://schools.ontotext.com/data/resource/place/>

#PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT distinct ?sch ?school ?province ?municipality ?town
WHERE {
#    ?place rdf:type schema:City;
#    	   rdfs:label ?placeName.
#    OPTIONAL {
#        ?place geo:sfWithin ?municipality.
#        ?municipality rdf:type :Municipality;
#                      rdfs:label ?municipalityName.
#        FILTER(lang(?municipalityName)="bg")
#   	}.
#
#    FILTER(lang(?placeName)="bg")

        ?sch schema:name ?school;
             :wikidata_entity ?school_wiki_id;
             :place ?place.

        ?place rdfs:label ?town;
                geo:sfWithin ?mun.

        ?mun geo:sfWithin ?prov;
             rdfs:label ?municipality.

        ?prov rdfs:label ?province.

        FILTER(lang(?town)="bg")
        FILTER(lang(?province)="bg")
    	FILTER(lang(?municipality)="bg")
}
ORDER BY ?province ?municipality ?town ?school



# work in progress

https://schools.ontotext.com/data/resource?uri=https:%2F%2Fschools.ontotext.com%2Fdata%2Fresource%2Fplace%2FQ192875
https://schools.ontotext.com/data/resource?uri=https:%2F%2Fschools.ontotext.com%2Fdata%2Fresource%2Fplace%2FQ2452987

PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX place: <https://schools.ontotext.com/data/resource/place/>

#SELECT ?place ?placeName ?municipality
SELECT *
WHERE {
#    ?municipality rdf:type :Municipality;
#                  rdf:type schema:AdministrativeArea;
#        		  rdfs:label ?municipalityName.
#
    ?place geo:sfWithin place:Q2452987.
#        rdf:type schema:Place;
#           rdf:type schema:AdministrativeArea;
#           rdf:type schema:City;
#           geo:sfWithin place:Q2452987;
#           rdfs:label ?placeName.
#
#    FILTER(lang(?placeName)="bg")
#    FILTER(lang(?municipalityName)="bg")
}


# Not working

PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX : <https://schools.ontotext.com/data/resource/ontology/>
BASE <https://schools.ontotext.com/data/resource/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX schema: <http://schema.org/>
SELECT *
  WHERE {
        ?s rdf:type qb:Observation;
           :date ?date;
           :grade_6 ?grade;
           :quantity_people ?num_pup;
           :school ?sch;
           :subject ?subject;
           :grade_level 12 .
} GROUP BY ?province ?town ?municipality ?school_wiki_id ?school ?year ?sub_group
ORDER BY ?province ?town ?school_wiki_id ?school ?year



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

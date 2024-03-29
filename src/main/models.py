from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey, Date

Models = MetaData()

Region = Table(
    'region',
    Models,
    Column('id', String(20), primary_key=True),
    Column('label', String(20))
)

Municipality = Table(
    'municipality',
    Models,
    Column('id', String(20), primary_key=True),
    Column('label', String(30), nullable=False),
    Column('region_id', ForeignKey('region.id'), nullable=False)
)

# TODO: how to distinguish city and villages? wikidata?
# TODO: do we want to import population data?
City: Table = Table(
    'city',
    Models,
    Column('id', String(20), primary_key=True),
    Column('label', String(30), nullable=False),
    # TODO: add this foriegn key after sfWithin predicate is fixed
    # Column('municipality_id', ForeignKey('municipality.id'), nullable=False)
)

School = Table(
    'school',
    Models,
    Column('id', String(20), primary_key=True),
    Column('name', String(30), nullable=False),
    Column('city_id', ForeignKey('city.id'), nullable=False)
)

# TODO: better name?
Dzi = Table(
    'dzi',
    Models,
    Column('id', String(20), primary_key=True),
    Column('date', Date, nullable=False),
    Column('name', String(20), nullable=False),
    Column('comment', String(50)),
)

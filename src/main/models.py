from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey

def define_db_schema():
    m = MetaData()

    Table(
        'regions',
        m,
        Column('id', String(20), primary_key=True),
        Column('label', String(20))
    )

    Table(
        'municipality',
        m,
        Column('id', String(20), primary_key=True),
        Column('label', String(30)),
        Column('region_id', ForeignKey('region.id'), nullable=False)
    )

    # TODO: how to distinguish city and villages? wikidata?
    Table(
        'city',
        m,
        Column('id', String(20), primary_key=True),
        Column('label', String(30)),
        Column('region_id', ForeignKey('municipality.id'), nullable=False)
    )

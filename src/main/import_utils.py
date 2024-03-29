import logging
from sqlalchemy import Table, Column, insert, select, update
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

def get_existing_objects(session: Session, id_column: Column):
    result = set()

    for row in session.execute(select(id_column).distinct()):
        result.add(row[0])

    return result


def prepare_for_insert_or_update(table: Table, raw_objects, existing_ids):
    to_insert = []
    to_update = []

    col_count = len(table.c)
    for obj in raw_objects:
        if len(obj) == col_count:
            if obj[0] in existing_ids:
                to_update.append(obj)
            else:
                to_insert.append(obj)


    logger.info(f'{table.name} to insert: {len(to_insert)}')
    logger.info(f'{table.name} to update: {len(to_update)}')

    return to_insert, to_update


def insert_objects(session: Session, table: Table, raw_objects):
    for raw_obj in raw_objects:
        values = {
            col: raw_obj[i]
            for i, col in enumerate(table.c.keys())
        }

        session.execute((
            insert(table)
            .values(values)
        ))

def update_objects(session: Session, table: Table, raw_objects):
    for raw_obj in raw_objects:
        values = {
            col: raw_obj[1:][i]
            for i, col in enumerate(table.c.keys()[1:])
        }

        session.execute((
            update(table)
            .where(table.c[0]==raw_obj[0])
            .values(values)
        ))

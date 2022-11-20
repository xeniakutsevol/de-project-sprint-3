from sqlalchemy.dialects.mysql import insert
from sqlalchemy import table

def insert_on_duplicate(sqltable, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy import table, column
    columns=[]
    for c in keys:
        columns.append(column(c))

    if sqltable.schema:
        table_name = '{}.{}'.format(sqltable.schema, sqltable.name)
    else:
        table_name = sqltable.name

    mytable = table(table_name, *columns)

    insert_stmt = insert(mytable).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(constraint='user_order_log_pk')

    conn.execute(do_nothing_stmt)
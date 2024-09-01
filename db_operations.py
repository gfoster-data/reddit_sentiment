import pandas as pd
import sqlite3
from sqlalchemy import create_engine


def read_sql(engine, sql_string: str):
    """
    :param engine: sqlalchemy engine with sqlite path:
    :param sql_string:
    :return: df
    """

    #read sql from sqlite db with pandas
    df = pd.read_sql(sql_string, engine)

    return df


def insert_records(engine, table_name: str, df):
    """
    :param engine: sqlalchemy engine object
    :param table_name: string of sqlite table name
    :param df: df
    :return: None
    """
    df.to_sql(table_name, engine, if_exists='append', index=False)
    return None


def make_table(engine, table_name: str, df):
    """
    :param engine: sqlalchemy engine object
    :param table_name: string of sqlite table name
    :param df: df
    :return: None
    """

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    return None


def delete_table(conn, table_name: str):
    """
    :param conn: sqlite3 connection object
    :param table_name: string
    :return: None
    """

    conn.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()
    conn.close()

    return None


def execute_sql(conn, sql: str):
    """
    USE AT YOUR OWN PERIL DZMITRY
    :param conn: sqlite3 connection object
    :param table_name: string
    :return: rows
    """

    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    if rows:
        df = pd.DataFrame(rows, columns=column_names)
        conn.close()
        return df
    elif 'select' in str.lower(sql):
        df = pd.DataFrame(columns=column_names)
        conn.close()
        return df
    else:
        conn.commit()
        conn.close()

    return None


if __name__ == '__main__':

    # # engine = create_engine('sqlite:///reddit_smp_sentiment.db')
    # # sql_string = 'SELECT * FROM y_finance_smp'
    # #
    # # df = read_sql(engine, sql_string)
    #
    conn = sqlite3.connect('reddit_smp_sentiment.db')
    df = execute_sql(conn, 'SELECT * FROM y_finance_smp')

    pass

    # from y_finance_query import query_spy
    #
    # start = '2024-08-01'
    # end = '2024-08-31'
    # interval = '1d'
    #
    # df = query_spy(start, end, interval)
    # engine = create_engine('sqlite:///reddit_smp_sentiment.db')
    # table_name = 'y_finance_smp'
    # insert_records(engine, table_name, df)
    #
    # pass





# # Create an engine connected to the SQLite database
# engine = create_engine('sqlite:///reddit_sentiment.db')
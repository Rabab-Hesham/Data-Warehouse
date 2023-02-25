import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop the tables in the database if exist
    Arguments:
    cur = the cursor object.
    conn = connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create the tables in the database
    Arguments:
    cur = the cursor object.
    conn = connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """Read configuration file"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    """Connection to the Redshift cluster"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    """Drop all the tables if exist"""
    drop_tables(cur, conn)
    
    """creating the tables"""
    create_tables(cur, conn)

    """closing the connection from the Redshift cluster"""
    conn.close()


if __name__ == "__main__":
    main()
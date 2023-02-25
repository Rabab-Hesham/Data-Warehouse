import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    load the data from s3 to the staging tables
    Arguments:
    cur = the cursor object.
    conn = connection to the database
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    inserting the data from the staging table to the tables in the warehause
    Arguments:
    cur = the cursor object.
    conn = connection to the DB
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    read configuration values from dwh.cfg file
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    """Connection to the Redshift cluster"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    """loading the data into the staging tables"""
    load_staging_tables(cur, conn)
    
    
    """inserting the data into the star schema"""
    insert_tables(cur, conn)
    
    
    """close the connection from the Redshift cluster"""
    conn.close()


if __name__ == "__main__":
    main()
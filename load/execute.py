import sys
import time
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging,format_time 


def create_spark_session(logger):
    """Initialize Spark session"""
    logger.info("stage0")
    return SparkSession.builder.appName("SpotifyDataTransform").getOrCreate()

def create_postgres_tables(logger, pg_un,pg_pw):
    """Create postgreSQL tables if they don't exit using pyscopg2"""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname ="postgres",
            user=pg_un,
            password=pg_pw,
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        logger.debug("Successfully connected to postgres sql")

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                track_id VARCHAR(50),
                track_name TEXT,
                track_popularity INTEGER,
                artist_id VARCHAR(50),
                artist_name TEXT,
                followers FLOAT,
                genres TEXT,
                artist_popularity INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT,
                related_ids TEXT[]
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendations_exploded (
            id VARCHAR(50),
            related_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_track (
            id VARCHAR(50),
            artist_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS track_metadata (
            id VARCHAR(50) PRIMARY KEY,
            name TEXT,
            popularity INTEGER,
            duration_ms INTEGER,
            danceability FLOAT,
            energy FLOAT,
            tempo FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_metadata (
            id VARCHAR(50) PRIMARY KEY,
            name TEXT,
            followers FLOAT,
            popularity INTEGER
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("POstgreSQL tables created successfully")
    
    except Exception as e:
        logger.warning(f"Error creating tables: {e}")
    finally:
        logger.debug("Closing connection and cursor to postgres db")
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_to_postgres(logger,spark, input_dir,pg_un,pg_pw):
    """Load Parquet files to PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/recommendations_exploded", "recommendations_exploded"),
        ("stage3/artist_track", "artist_track"),
        ("stage3/track_metadata", "track_metadata"),
        ("stage3/artist_metadata", "artist_metadata"),
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "append" if 'master' in parquet_path else "overwrite"
            df.write \
                .mode(mode) \
                .jdbc(url = jdbc_url, table = table_name, properties=connection_properties)
            logger.info(f"Loaded {table_name} to PostgresSQL ")
        except Exception as e:
            logger.warning(f"Error loading {table_name}: {e}")
        
if __name__ == "__main__":

    logger=setup_logging("load.log")
    if len(sys.argv) != 7:
        print("Usage: python load/execute.py <input_dir> <pg_un> <pg_pw>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    pg_un=sys.argv[2]
    pg_pw=sys.argv[3]
    
    if not os.path.exists(input_dir):
        logger.error(f"ErrorL input directory {input_dir} does not exist")
        sys.exit(1)

    logger.info("Load stage started")
    start=time.time()

    spark = create_spark_session(logger)
    create_postgres_tables(logger,pg_un,pg_pw)
    load_to_postgres(logger,spark, input_dir,pg_un,pg_pw)


    end=time.time()
    logger.info("Loading Stage COmpleted")
    logger.info(f"Total time taken {format_time(end-start)}")
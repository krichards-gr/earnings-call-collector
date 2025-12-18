import sqlite3
import pandas as pd
import os

DB_NAME = 'transcripts.db'

def get_connection():
    return sqlite3.connect(DB_NAME)

def initialize_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Create metadata table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcript_metadata (
            transcript_id TEXT PRIMARY KEY,
            symbol TEXT,
            report_date TEXT,
            fiscal_year INTEGER,
            fiscal_quarter INTEGER
        )
    ''')
    
    # Create content table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcript_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transcript_id TEXT,
            paragraph_number INTEGER,
            speaker TEXT,
            content TEXT,
            FOREIGN KEY (transcript_id) REFERENCES transcript_metadata (transcript_id)
        )
    ''')
    
    conn.commit()
    conn.close()

def get_existing_ids():
    if not os.path.exists(DB_NAME):
        return set()
    
    conn = get_connection()
    query = "SELECT transcript_id FROM transcript_metadata"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return set(df['transcript_id'].tolist())

def insert_metadata(df):
    conn = get_connection()
    df.to_sql('transcript_metadata', conn, if_exists='append', index=False)
    conn.close()

def insert_content(df):
    conn = get_connection()
    df.to_sql('transcript_content', conn, if_exists='append', index=False)
    conn.close()

if __name__ == "__main__":
    initialize_db()
    print(f"Database {DB_NAME} initialized.")

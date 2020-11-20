import sqlite3
import os

if not os.path.exists('Database'):
    os.makedirs('Database')

con = sqlite3.connect('Database/netimdb.db')
cur = con.cursor()

cur.executescript("""
CREATE TABLE IF NOT EXISTS 'movies_id' ( 'id'VARCHAR, 'title'VARCHAR, 'year'INTEGER, 'type'VARCHAR, 'rating'INTEGER, 'num'INTEGER, PRIMARY KEY('id') );
CREATE TABLE IF NOT EXISTS netflix (id INTEGER PRIMARY KEY, title VARCHAR, year INTEGER);
CREATE TABLE IF NOT EXISTS netflixYear (id INTEGER PRIMARY KEY, year INTEGER);
CREATE TABLE IF NOT EXISTS ratings (id VARCHAR PRIMARY KEY, rating REAL, num INTEGER);
CREATE TABLE IF NOT EXISTS watched (id VARCHAR PRIMARY KEY);
""")


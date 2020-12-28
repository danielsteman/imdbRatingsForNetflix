import json
import requests
from bs4 import BeautifulSoup
import re
import csv
import os
import gzip
import shutil
import sqlite3
import sys
import math
import pandas as pd
import numpy as np
from updateFunctions import updateRatings, updateMovies_id, updateNetflix, updateNetflixYears, updateOwnRatings

csv.field_size_limit(sys.maxsize)

updateRatings()
updateMovies_id()
updateNetflix()
updateNetflixYears()
updateOwnRatings()

"""
Query to get ratings for Netflix movies that haven't been rated:

SELECT a.title, a.year, b.rating 
FROM netflix a 
LEFT JOIN movies_id b 
ON a.title = b.title AND a.year = b.year 
WHERE b.type IS 'movie'
AND b.id NOT IN (SELECT id FROM watched)
AND b.num > 5000
ORDER BY b.rating DESC;
"""

con = sqlite3.connect('Database/netimdb.db')

dfOut = pd.read_sql_query("SELECT a.title, a.year, b.rating FROM netflix a LEFT JOIN movies_id b ON a.title = b.title AND a.year = b.year WHERE b.type IS 'movie' AND b.id NOT IN (SELECT id FROM watched) AND b.num > 5000 ORDER BY b.rating DESC", con)

con.commit()
con.close()

dfOut.to_csv("output/toBeSeen.csv", sep=';')
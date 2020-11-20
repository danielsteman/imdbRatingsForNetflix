def download(url):
    
    # download file from URL and write to new .tsv file
    filename = url.split("/")[-1]
    with open(filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    
    return

# Gunzip file
def gunzip(filename):
    
    unzipPath = os.getcwd() + "/" + filename
    with gzip.open(unzipPath, 'rb') as f_in:
        # file name is name of zipped file without file extension
        newFilename = ''.join(filename.split(".")[0:2])
        with open(f'{newFilename}.tsv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    # get rid of zip file after unpacking
    os.remove(unzipPath)
    
    return

def datasetURLs():
    
    # Get URL's of downloadable IMDB datasets
    urllist = []
    datasetUrl = "https://datasets.imdbws.com/"
    response = requests.get(datasetUrl)
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("ul")
    for link in links:
        url = link.find("a", href=True)['href']
        urllist.append(url)
        
    return urllist

def updateIMDBdatasets():
    # download IMDB datasets, unzip and delete zip
    imdbDataFileNames = [
        "title.basics.tsv.gz",
        "title.ratings.tsv.gz"
    ]

    for file in imdbDataFileNames:
        download("https://datasets.imdbws.com/" + file)
        gunzip(file)
        
    return

def updateMovies_id():
    
    # connect with database
    con = sqlite3.connect('Database/netimdb.db')
    cur = con.cursor()
    
    with open("titlebasics.tsv") as file:
        rows = csv.reader(file, delimiter="\t")
        # don't insert header as row
        next(rows)
        data = [(row[0], row[1], row[2], row[5]) for row in rows]
    
    cur.executemany("REPLACE INTO movies_id (id, type, title, year) VALUES (?, ?, ?, ?);", data)
    
    con.commit()
    con.close()
    
    return

def updateRatings():
    
    # connect with database
    con = sqlite3.connect('Database/netimdb.db')
    cur = con.cursor()
    
    with open("titleratings.tsv") as file:
        rows = csv.reader(file, delimiter="\t")
        # don't insert header as row
        next(rows)
        data = [(row[0], row[1], row[2]) for row in rows]

    cur.executemany("REPLACE INTO ratings (id, rating, num) VALUES (?, ?, ?);", data)
    cur.execute("UPDATE movies_id SET rating = (SELECT rating FROM ratings WHERE ratings.id = movies_id.id)")
    cur.execute("UPDATE movies_id SET num = (SELECT num FROM ratings WHERE ratings.id = movies_id.id)")
    
    con.commit()
    con.close()
    
    return

def getNetflixTitles(genreCode):
    
    # load webpage
    url = f"https://www.netflix.com/browse/genre/{str(genreCode)}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    txt = str(soup.find('script'))
    
    titlePattern = "(?<=\"name\":\")(.*?)(?=\",)"
    codePattern = "(?<=\"url\":\")(.*?)(?=\"}})"
    
    # list all Netflix titles
    titleList = re.findall(titlePattern, txt)
    
    # list all Netflix URLs
    codeList = re.findall(codePattern, txt)
    
    # extract Netflix movie code from Netflix URL
    codeListClean = [code.split("/")[-1] for code in codeList]
    
    # skip title header of title list and combine with code list in tuples
    data = zip(titleList[1:], codeListClean)

    return list(data)

def getNetflixYear(code):
    
    pattern = "(?<=\"item-year\">)(.*?)(?=<)"
    url = f"https://www.netflix.com/nl-en/title/{code}"
    txt = requests.get(url).text
    year = re.findall(pattern, txt)
    
    return year

def updateNetflix():
    
    # connect with database
    con = sqlite3.connect('Database/netimdb.db')
    cur = con.cursor()
    
    # for each category code, scrape titles 
    for catCode in [cat[1] for cat in categories.items()]:
        data = getNetflixTitles(catCode)
        # insert or update titles
        cur.executemany("REPLACE INTO netflix (title, id) VALUES (?, ?);", data)

    con.commit()
    con.close()
    
    return

def updateNetflixYears():
    
    con = sqlite3.connect('Database/netimdb.db')
    cur = con.cursor()

    # Query netflix ids that don't have a year yet
    netflixIdQuery = cur.execute("SELECT id FROM netflix EXCEPT SELECT id FROM netflixYear;").fetchall()
    
    # If all ids have a year, we're done
    if netflixIdQuery == None:
        return

    netflixId = [int(Id[0]) for Id in netflixIdQuery]

    data = []
    
    for Id in netflixId:
        try:
            data.append(int(getNetflixYear(Id)[0]))
        except:
            data.append("NULL")
    
    zippedData = list(zip(netflixId, data))

    cur.executemany("REPLACE INTO netflixYear (id, year) VALUES (?, ?);", zippedData)
    cur.execute("UPDATE netflix SET year = (SELECT year FROM netflixYear WHERE netflix.id = netflixYear.id)")

    con.commit()
    con.close()
    
    return

def updateOwnRatings():
    
    idList = []

    website = "https://www.imdb.com/"
    nextPage = f"/user/ur27266239/ratings"
    idPattern = "(?<=data-tconst=\")(.*?)(?=\")"

    while nextPage != None:

        url = website + nextPage
        response = requests.get(url).text

        ids = re.findall(idPattern, response)
        idList.append(ids)

        soup = BeautifulSoup(response, "html.parser")
        nextUrlElement = soup.find_all("a", class_="flat-button lister-page-next next-page")

        try:
            nextPage = [i['href'] for i in nextUrlElement][0]
        except:
            nextPage = None

    flat_list = [item for sublist in idList for item in sublist]
    
    con = sqlite3.connect('Database/netimdb.db')
    cur = con.cursor()

    cur.executemany("REPLACE INTO watched (id) VALUES (?);", list(zip(flat_list)))

    con.commit()
    con.close()
    
    return


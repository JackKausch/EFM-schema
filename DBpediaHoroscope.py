import requests
from lookup import DBpediaLookup
from isub import isub
from scipy import spatial
from kerykeion import KrInstance, MakeSvgInstance
import ast
from SPARQLWrapper import SPARQLWrapper, JSON
import time
import pandas as pd


dbpedia = DBpediaLookup()
file = 'collectie_BPH.csv'
dataframe = pd.read_csv(file, sep=',',quotechar='"',escapechar="\\")

def getEmbeddings(uri):
    
    #Check http://www.kgvec2go.org/
        
    kg_entity = uri
    
    r = requests.get('http://www.kgvec2go.org/rest/get-vector/dbpedia/' + kg_entity) 

    if r.text == "{}":

        p = requests.get('http://www.kgvec2go.org/rest/get-vector/dbpedia/Earth')

        return p.text
        
    else:

        return r.text


firstVector = getEmbeddings("Hermeticism")
alchemy = ast.literal_eval(firstVector)
uris = []

def getExternalKGURI(name):
    
    entities = dbpedia.getKGEntities(name, 10)
    current_sim = -1
    current_uri=''
    
    for ent in entities:
        
        name2 = ent.label.replace("<B>","").replace("</B>","").replace(" ","_").replace("/","_") #removes html tags from the label

        vector = ast.literal_eval(getEmbeddings(name2)) #gets an embedding representation of the label
        
        isub_score = isub(name, ent.label) #gets an isub score for the label
        
        dist = spatial.distance.cosine(vector["vector"],alchemy["vector"]) #takes the cosine distance between alchemy and the label

        combined = ((dist/10)+isub_score) #combines the string matching and semantic matching into one metric
                                          #the weighting of this metric is in favor of isub, embeddings used only to disambiguate
        if current_sim < combined:
            
            current_uri = ent.ident
            current_sim = combined
    uris.append(current_uri)
    return(current_uri)
link = []

def queryRemoteGraph(endpoint_url, query, attempts=3):
    
    sparqlw = SPARQLWrapper(endpoint_url)        
    sparqlw.setReturnFormat(JSON)
    
       
    try:
            
        sparqlw.setQuery(query)
            
        results = sparqlw.query().convert()
        
        #Prints JSON file
        #print(results)
                   
        for result in results["results"]["bindings"]:
            
            #Prints individual results 
            print(result["x"]["value"])
            return(result["x"]["value"])
             
    except:
            
        print("Query '%s' failed. Attempts: %s" % (query, str(attempts)))
        time.sleep(60) #to avoid limit of calls, sleep 60s
        attempts-=1
        if attempts>0:
            return queryRemoteGraph(endpoint_url, query, attempts)
        else:
            return None


birthYear = []
deathYear = []


for author in dataframe["Author"]:
    URI = str(getExternalKGURI(str(author)))
    print(URI)
    dbpedia_query = "SELECT DISTINCT ?x WHERE { <%s> <http://dbpedia.org/ontology/birthDate> ?x . }" %URI
    dbpedia_query2 = "SELECT DISTINCT ?x WHERE { <%s> <http://dbpedia.org/ontology/birthPlace> ?y . ?y <http://dbpedia.org/property/name> ?x}" %URI
    dbpedia_query3 = "SELECT DISTINCT ?x WHERE { <%s> <http://dbpedia.org/ontology/birthYear> ?x . }" %URI
    dbpedia_query4 = "SELECT DISTINCT ?x WHERE { <%s> <http://dbpedia.org/ontology/deathYear> ?x . }" %URI
    dbpedia_endpoint = "http://dbpedia.org/sparql"  #this SPARQL query gets the original wikipedia article to be scraped 
    astro = queryRemoteGraph(dbpedia_endpoint, dbpedia_query)
    birthplace = queryRemoteGraph(dbpedia_endpoint, dbpedia_query2)
    birth = queryRemoteGraph(dbpedia_endpoint, dbpedia_query3)
    death = queryRemoteGraph(dbpedia_endpoint, dbpedia_query4)

    if birth is None:
        birthYear.append("Unknown")
    else:
        birthYear.append(birth)

    if death is None:
        deathYear.append("Unknown")
    else:
        deathYear.append(death)


    if astro is None:
        pass
    else:
        date = astro.split("-")
        if birthplace is None:
            birthplace = "Unknown"
            horoscope = KrInstance(str(author), int(date[0]),int(date[1]),int(date[2]),12,12, birthplace)
            chart = MakeSvgInstance(horoscope, chart_type="Natal")
            chart.makeSVG()
            print(len(chart.aspects_list))
        elif birthplace == "Lviv":
            birthplace = "Unknown"
            horoscope = KrInstance(str(author), int(date[0]),int(date[1]),int(date[2]),12,12, birthplace)
            chart = MakeSvgInstance(horoscope, chart_type="Natal")
            chart.makeSVG()
            print(len(chart.aspects_list))
        else:
            horoscope = KrInstance(str(author), int(date[0]),int(date[1]),int(date[2]),12,12, birthplace)
            chart = MakeSvgInstance(horoscope, chart_type="Natal")
            chart.makeSVG()
            print(len(chart.aspects_list))

#dataframe['dbpedia'] = uris
dataframe['Birth Year'] = birthYear
dataframe['Death Year'] = deathYear
dataframe.to_csv('AuthorBirthDeath.csv')


    


    





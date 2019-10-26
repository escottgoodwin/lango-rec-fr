import os
from bs4 import BeautifulSoup
import json
from textblob import TextBlob
from gensim.models.doc2vec import Doc2Vec,TaggedDocument
import datetime
from datetime import datetime,timedelta
import requests
from stop_words import get_stop_words
import psycopg2

lang = 'fr'

dbname=os.environ['PGCONNECT_DBNAME']
user=os.environ['PGCONNECT_USER']
password=os.environ['PGCONNECT_PASSWORD']
host= os.environ['PGCONNECT_HOST']
port=os.environ['PGCONNECT_PORT']

lang_model = Doc2Vec.load(lang + 'model3.model')

def art_parser(link):
    r = requests.get(link)
    page = r.text
    soup = BeautifulSoup(page,"lxml")
    for x in soup('script'):
        x.decompose()
    for x in soup('link'):
        x.decompose()
    for x in soup('meta'):
        x.decompose()
    title = soup.title.string
    paras = soup('p')
    atriclestrip = [art.get_text() for art in paras]
    art = ' '.join(atriclestrip)
    return art,link,title

def link_search(link):

    art,link,title = art_parser(link)

    trans_art = [str(TextBlob(art).translate(to=lang))]

    langt = 'French'

    stop_words = get_stop_words(lang)
    histnostop = [[i for i in doc.lower().split() if i not in stop_words] for doc in trans_art]
    dlhist_tagged = [TaggedDocument(doc,[i]) for i,doc in enumerate(histnostop)]
    trans_lang_vec = [lang_model.infer_vector(doc.words) for doc in dlhist_tagged]
    rec_num = 20
    sims = lang_model.docvecs.most_similar(trans_lang_vec, topn=rec_num)
    sims1= [x[0] for x in sims]
    sims2= tuple(sims1)
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port, sslmode='require')
    cur = conn.cursor()
    sql="SELECT link,title,art_id,dt FROM " + lang + "_arts WHERE art_id IN %s"
    cur.execute(sql,(sims2,))
    recs = cur.fetchall()
    dictrecs = [{'link':x[0],'title':x[1],'art_id':x[2],'date':str(x[3])} for x in recs]
    conn.close()
    payload = {'recs':dictrecs,'link':link,'title':title,'trans_lang':lang,'langt':langt}
    resp=json.dumps(payload)
    return resp

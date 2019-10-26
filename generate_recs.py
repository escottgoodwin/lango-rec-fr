from gensim.models.doc2vec import Doc2Vec,TaggedDocument
import datetime
import json
from textblob import TextBlob
from stop_words import get_stop_words
import os
import re
import sys 
import numpy as np 
import psycopg2
import requests
from bs4 import BeautifulSoup
import traceback
from requests.exceptions import HTTPError

user = os.getenv('PGCONNECT_USER')
password = os.getenv('PGCONNECT_PASSWORD')
host = os.getenv('PGCONNECT_HOST')
port = os.getenv('PGCONNECT_PORT')
dbname = os.getenv('PGCONNECT_DBNAME')
cloud_link = os.getenv('GCLOUD_LINK')

def article_vec(article,lang_model,lang):
    stop_words = get_stop_words(lang)
    histnostop = [i for i in article.lower().split() if i not in stop_words]
    vec = lang_model.infer_vector(histnostop)
    return vec

def trans_vec_centers(cluster, trans_lang_model, trans_lang):
    trans_vecs=[]
    for art in cluster["cluster"]:
        try:
            trans_art = str(TextBlob(art["art"]).translate(to=trans_lang))
            trans_vec = article_vec(trans_art, trans_lang_model, trans_lang)
            trans_vecs.append(trans_vec)
        except Exception:
            traceback.print_exc()
            pass
            
    vec_center = np.mean(trans_vecs, axis = 0)
    return vec_center

def get_recs(vec_centers,lang_model,rec_num):
    recs = [lang_model.docvecs.most_similar([vec_centers[i]], topn=rec_num) for i, x in enumerate(vec_centers)]
    return [[x[0] for x in rec] for rec in recs]

def store_recs(uid, recs, trans_lang, cluster):
    now = datetime.datetime.now()
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port, sslmode='require')
    cur = conn.cursor() 
    for art_id in recs:
        data = (uid, art_id, now, trans_lang, cluster)
        query = 'INSERT INTO recommendations (uid, art_id, rec_date, lang, cluster_num) VALUES (%s,%s,%s,%s,%s)'
        conn.commit()
        cur.execute(query,data)
    conn.close()

def generate_recs(pop_clusters,uid, trans_lang, rec_num):
    trans_lang_model_path=f'{trans_lang}model3.model'

    t0=datetime.datetime.now()
    trans_lang_model = Doc2Vec.load(trans_lang_model_path)
    t1=datetime.datetime.now()
    loaded_models = f'loaded models {str(t1-t0)}'
    print(loaded_models)

    t14=datetime.datetime.now()
    trans_lang_vec_centers = [trans_vec_centers(cluster,trans_lang_model,trans_lang) for cluster in pop_clusters]
    t15=datetime.datetime.now()
    trans_vec='translate and vec to trans lang'+ str(t15-t14)
    print(trans_vec)

    t16=datetime.datetime.now()
    recs = get_recs(trans_lang_vec_centers, trans_lang_model, rec_num)
    t17=datetime.datetime.now()
    trans_recs = 'get trans lang recs' + str(t17-t16)
    print(trans_recs)

    t18=datetime.datetime.now()
    for i,recs in enumerate(recs):
        store_recs(uid, recs, trans_lang, i)
    
    t19=datetime.datetime.now()
    store_recs1='store recs' + str(t19-t18)
    print(store_recs1)
    total='total time'+str(t19-t0)
    print(total)

    generation_times={
        "loaded_models":loaded_models,
        "trans_vec":trans_vec,
        "trans_recs":trans_recs,
        "store_recs":store_recs1,
        "total":total
    }
    return generation_times

def main():

    native_lang = sys.argv[1]
    trans_lang = sys.argv[2]
    uid = sys.argv[3]
    clust_num=15
    percent=.33
    rec_num=20 
    
    print('generating recs for userid:' + uid + ' trans: '+ trans_lang)

    cluster_link = f'https://lango-cluster-{native_lang}-{cloud_link}-uc.a.run.app/get_recs'

    try:
        response = requests.post(cluster_link, json={
            "native_lang": native_lang,
            "uid":uid,
            "clust_num":clust_num,
            "percent":percent
        })
        pop_clusters=response.json()
        generation_times = generate_recs(pop_clusters,uid, trans_lang, rec_num)

        print(generation_times)
        
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  
    except Exception as err:
        print(f'Other error occurred: {err}') 
    else:
        print('Success!')

if __name__ == '__main__':
    main()

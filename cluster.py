from gensim.models.doc2vec import Doc2Vec,TaggedDocument
import datetime
import json
from textblob import TextBlob
import pandas as pd
from sklearn.cluster import KMeans
from stop_words import get_stop_words
import os
import re
import sys 
import numpy as np 
import psycopg2
import requests
from bs4 import BeautifulSoup
import traceback

user = os.getenv('PGCONNECT_USER')
password = os.getenv('PGCONNECT_PASSWORD')
host = os.getenv('PGCONNECT_HOST')
port = os.getenv('PGCONNECT_PORT')
dbname = os.getenv('PGCONNECT_DBNAME')

def fetch_user_links(uid):
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port, sslmode='require')
    cur = conn.cursor()
    sql = f"SELECT link FROM user_links WHERE uid LIKE '{uid} AND date > now() - INTERVAL '3 days'"
    cur.execute(sql)
    links  = cur.fetchall()
    conn.close()
    newlinks = [x[0] for x in links]
    return newlinks

def get_feed_articles(links,lang,min_len=2000,max_len=50000):
    count = 0
    articles = []
    for link in links:
        try:
            art = art_parser(link)
            if (min_len <= len(art) <= max_len):
                articles.append(art)
        except Exception:
            print(link)
            traceback.print_exc()
            pass
        count += 1
        pct = count / len(links)
        print(pct, end='\r')
        sys.stdout.flush()
    return articles

def art_parser(link):
    r = requests.get(link,timeout=(1,4))
    page = r.text
    soup = BeautifulSoup(page,"lxml")
    for x in soup('script'):
        x.decompose()
    for x in soup('link'):
        x.decompose()
    for x in soup('meta'):
        x.decompose()
    paras = soup('p')
    if soup.title is None:
        prep_art = ''
        return prep_art
    else:
        atriclestrip = [art.get_text() for art in paras]
        prep_art = ' '.join(atriclestrip)
        return prep_art

def one_lang(arts,native):
    arts1_cur_1lang = []
    for x in arts:
        art = TextBlob(x)
        if art.detect_language() != native:
            trans = art.translate(to=native)
            arts1_cur_1lang.append(str(trans))
        else:
            arts1_cur_1lang.append(x)
    return arts1_cur_1lang

def clean_arts(arts,native):
    no_pdf = [x for x in arts if not x.startswith("%PDF-")]
    #one_lang1 = one_lang(no_pdf, native)
    return no_pdf

def article_vec(article,lang_model,lang):
    stop_words = get_stop_words(lang)
    histnostop = [i for i in article.lower().split() if i not in stop_words]
    vec = lang_model.infer_vector(histnostop)
    return vec

def cluster_articles(articles,vecs,clust_num):
    km = KMeans(n_clusters=clust_num)
    km.fit(vecs)
    cluster_labels = km.labels_.tolist()
    df = pd.DataFrame(cluster_labels,columns=['cluster']) 
    df['native_vector'] = vecs 
    df['native_article'] = articles 
    cluster_grps = [df.loc[df['cluster'] == x]['native_article'].tolist() for x in range(clust_num)]
    return cluster_grps

def popular_clusters(cluster_grps,percent):
    sorted_clusters= sorted(cluster_grps, key=len,reverse=True)
    pop_length = int(len(cluster_grps)*percent)
    return sorted_clusters[:pop_length]

def pop_clust_output(sorted_clusters):
    pop1_clusts = []
    for cluster in sorted_clusters:
        arts = []
        for art in cluster:
            art1 = {"art":art}
            arts.append(art1)
        cluster = {"cluster":arts}
        pop1_clusts.append(cluster)
    return pop1_clusts

def cluster(native_lang,uid,clust_num,percent):

    native_lang_model_path=f'{native_lang}model3.model'

    print('generating pop arts for userid:' + uid + ' native: '+ native_lang)

    t0=datetime.datetime.now()

    native_lang_model = Doc2Vec.load(native_lang_model_path)

    t1=datetime.datetime.now()
    loaded_models = f'loaded models {str(t1-t0)}'
    print(loaded_models)
    
    t2=datetime.datetime.now()

    links = fetch_user_links(uid)

    t3=datetime.datetime.now()
    loaded_links = f'loaded links {str(len(links))} {str(t3-t2)}'
    print(loaded_links)
    
    t4=datetime.datetime.now()

    articles = get_feed_articles(links, native_lang, min_len=2000, max_len=50000)

    t5=datetime.datetime.now()
    loaded_art='loaded arts ' + str(len(articles)) + ' ' + str(t5-t4)
    print(loaded_art)

    t6=datetime.datetime.now()

    cleaned_arts = clean_arts(articles,native_lang)

    t7=datetime.datetime.now()
    cleaned_arts1='cleaned arts ' + str(len(cleaned_arts)) + ' ' + str(t7-t6)
    print(cleaned_arts1)

    t8=datetime.datetime.now()

    native_art_vecs =[article_vec(article,native_lang_model,native_lang) for article in cleaned_arts]

    t9=datetime.datetime.now()
    native_vec='native vec arts '+str(t9-t8)
    print(native_vec)

    t10=datetime.datetime.now()

    cluster_grps = cluster_articles(cleaned_arts, native_art_vecs, clust_num)

    t11=datetime.datetime.now()
    cluster_native='cluster native arts '+ str(t11-t10)
    print(cluster_native)

    t12=datetime.datetime.now()

    pop_clusters = popular_clusters(cluster_grps, percent)

    t13=datetime.datetime.now()
    popular_clust='popular native clusters '+ str(t13-t12)
    print(popular_clust)
    
    total='total time'+str(t13-t0)
    print(total)

    pop_clust_output1 = pop_clust_output(pop_clusters)
    
    return pop_clust_output1

def main():

    native_lang = sys.argv[1]
    uid = sys.argv[2]
    clust_num=15
    percent=.33

    print('generating recs for userid:' + uid + ' native: '+ native_lang)

    generation_times = cluster(native_lang,uid,clust_num,percent)
    print(generation_times)

if __name__ == '__main__':
    main()

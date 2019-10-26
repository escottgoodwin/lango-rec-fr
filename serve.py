import os
import datetime
from datetime import datetime,timedelta
from flask import Flask, request
from d2v import link_search
from cluster import cluster
import json
from generate_recs import generate_recs

application = Flask(__name__)

def list_routes():
    return ['%s' % rule for rule in application.url_map.iter_rules()]

@application.route("/")
def routes():
    routelinks = list_routes()
    html = "<h1 style='color:blue'>French Routes</h1>"
    for link in routelinks:
        html += '<P><H3>'+link+'</H3></P>'
    
    return html

@application.route("/link_search", methods=['POST'])
def link_search_pg():
    link = request.json['link']

    resp = link_search(link)

    return resp

@application.route("/cluster", methods=['POST'])
def cluster_recs():
    native_lang = request.json['native_lang']
    uid = request.json['uid']
    clust_num = request.json['clust_num']
    percent = request.json['percent']

    pop_clusters = cluster(native_lang,uid,clust_num,percent)

    return json.dumps(pop_clusters)

@application.route("/get_recs", methods=['POST'])
def get_recs():
    trans_lang = request.json['trans_lang']
    uid = request.json['uid']
    rec_num = request.json['rec_num']
    pop_clusters=request.json['pop_clusters']

    generation_times = generate_recs(pop_clusters,uid, trans_lang, rec_num)

    return json.dumps(generation_times)

if __name__ == '__main__':
    application.run(debug=True,host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
import os
import datetime
from datetime import datetime,timedelta
from flask import Flask, request
from d2v import link_search

application = Flask(__name__)

lang = 'es'

def list_routes():
    return ['%s' % rule for rule in application.url_map.iter_rules()]

@application.route("/")
def routes():
    routelinks = list_routes()
    html = "<h1 style='color:blue'>French Routes</h1>"
    for link in routelinks:
        html += '<P><H3>'+link+'</H3></P>'
    
    return html

@application.route("/apis/link_search", methods=['POST'])
def link_search_pg():
    link = request.json['link']

    resp = link_search(link)

    return resp

if __name__ == '__main__':
    application.run(debug=True,host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
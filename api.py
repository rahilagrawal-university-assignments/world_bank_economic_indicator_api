from flask import Flask, request, g, jsonify
from flask_restplus import Resource, Api, fields
import sqlite3
import requests
from datetime import datetime
import re

app = Flask(__name__)
api = Api(app)
order_by_details = api.parser().add_argument('order_by', help="Comma separated string value to sort the collection based on the given criteria. In each segment + indicates ascending order, and - indicates descending order", location='args')
query_details = api.parser().add_argument('query', help="+10 for top 10, -10 for bottom 10",location='args')
indicator_details = api.parser().add_argument('indicator_id', help="indicator_id : an indicator http://api.worldbank.org/v2/indicators",location='args')

@api.route('/collections')
class Collections(Resource):
    # @api.response(200, 'Success', indicator_details)
    # @api.response(404, 'Invalid ID')
    @api.expect(indicator_details)
    @api.doc(description='''
        This operation can be considered as an on-demand 'import' operation. 
        The service will download the JSON data for all countries respective
        to the year 2012 to 2017 and identified by the indicator id given by
        the user
    ''')
    def post(self) :
        # Validate indicator id
        indicator_id = request.args.get('indicator_id')
        if not indicator_id:
            api.abort(404, "Indicator Id not provided")
        
        # send request to external api
        URL = "http://api.worldbank.org/v2/countries/all/indicators/{}?date=2012:2017&format=json&per_page=1000".format(indicator_id)
        resp = requests.get(url=URL)
        if not resp:
            api.abort(404, "Did not get valid response for given indicator ID")

        if len(resp.json()) < 2:
            api.abort(404, "No country data found for given indicator between 2012 and 2017")
        
        response = (resp.json())[1]
        indicator = (response[0])["indicator"]
        indicator_value = indicator["value"]
        created_at = datetime.now()
        db = get_db()
        cur = db.cursor()
        cur.execute('SELECT ID FROM COLLECTIONS WHERE INDICATOR = ?', (indicator_id,))
        row = cur.fetchone()
        if row is not None:
            collection_id = row[0]
            cur.execute('DELETE FROM COLLECTIONS WHERE INDICATOR = ?', (indicator_id,))
            cur.execute('DELETE FROM COUNTRIES WHERE COLLECTION_ID = ?', (collection_id,))
        cur.execute('INSERT INTO COLLECTIONS(INDICATOR, CREATION_TIME, INDICATOR_VALUE) values (?, ?, ?)', (indicator_id, created_at, indicator_value))
        collection_id = cur.lastrowid
        countryInserts = []
        for country in resp.json()[1]:
            if country["value"] == None:
                continue
            countryInserts.append(((country["country"])["value"], country["date"], country["value"], collection_id))
        cur.executemany('INSERT INTO COUNTRIES(COUNTRY, DATE, VALUE, COLLECTION_ID) values (?,?,?, ?)', countryInserts)

        db.commit()
        collection_id = cur.lastrowid
        
        return jsonify({
            "uri": "/collections/{}".format(collection_id), 
            "id": collection_id,  
            "creation_time": "{}".format(created_at),
            "indicator_id" : indicator_id
        })

    @api.expect(order_by_details)
    @api.doc(description='''Retrieve the list of available collections with the ability to order them by different fields''')
    def get(self):
        db = get_db()
        cur = db.cursor()
        queryString = 'SELECT * FROM COLLECTIONS'
        query = request.args.get('order_by').replace(" ", "")
        orders = query.split(",") if query is not None else []
        print(orders)
        if len(orders) > 0:
            queryString = queryString + " ORDER BY"
            for order in orders:
                if not len(order) > 0:
                    continue
                queryString = queryString + " " + order[1:].upper()
                queryString = (queryString + " DESC,") if order[0] == '-' else (queryString + " ASC,")
            queryString = queryString[:len(queryString)-1]
        cur.execute(queryString)
        rows = cur.fetchall()
        collections = []
        for row in rows:
            collections.append({ 
                "uri": "/collections/{}".format(row[0]), 
                "id": row[0],  
                "creation_time": "{}".format(row[2]),
                "indicator_id" : row[1]
            })
        return jsonify(collections)
            

@api.route('/collections/<id>')
class CollectionsId(Resource):
    @api.doc(description='''This operation deletes an existing collection from the database''')
    def delete(self, id):
        db = get_db()
        cur = db.cursor()
        cur.execute('DELETE FROM COLLECTIONS WHERE ID = ?', (id,))
        cur.execute('DELETE FROM COUNTRIES WHERE COLLECTION_ID = ?', (id,))
        db.commit()
        return jsonify({
            "message" :"The collection {} was removed from the database!".format(id),
            "id": id
        })

    @api.doc(description='''
        This operation retrieves a collection by its ID .
        The response of this operation will show the imported
        content from world bank API for all 6 years
    ''')
    def get(self, id):
        db = get_db()
        cur = db.cursor()
        cur.execute('SELECT * FROM COLLECTIONS WHERE ID = ?', (id,))
        collection = cur.fetchone()
        if collection == None:
            return api.abort(400, "No data found for given id")
        cur.execute('SELECT * FROM COUNTRIES WHERE COLLECTION_ID = ?', (id,))
        rows = cur.fetchall()
        entries = []
        for row in rows:
            entries.append({
                "country": row[1],
                "date": row[2],
                "value": row[3]
            })
        return jsonify({
            "id": id,
            "indicator_id": collection[1],
            "indicator_value": collection[3], 
            "creation_time": "{}".format(collection[2]),
            "entries": entries
        })

@api.route('/collections/<id>/<year>/<country>')
class CollectionsIdYearCountry(Resource):
    @api.doc(description='''Retrieve economic indicator value for given country and a year''')
    def get(self, id, year, country):
        db = get_db()
        cur = db.cursor()
        cur.execute('SELECT * FROM COLLECTIONS WHERE ID = ?', (id,))
        collection = cur.fetchone()
        if collection == None:
            return api.abort(400, "No data found for given id")
        cur.execute('SELECT * FROM COUNTRIES WHERE COLLECTION_ID = ? AND DATE = ? AND COUNTRY = ?', (id, year, country,))
        row = cur.fetchone()
        if row == None:
            return  api.abort(400,"No data found for given combination of id, year and country")
        return jsonify({
            "id": id,
            "indicator": collection[1],
            "country": row[1],
            "date": row[2],
            "value": row[3]
        })

@api.route('/collections/<id>/<year>')
class CollectionsIdYear(Resource):
    @api.expect(query_details)
    @api.doc(description='''
        The <query> is an optional integer parameter which can be either of following:
        +N (or simply N) : Returns top N countries sorted by indicator value (highest first)
        -N : Returns bottom N countries sorted by indicator value
    ''')
    def get(self, id, year):
        db = get_db()
        cur = db.cursor()
        cur.execute('SELECT INDICATOR, INDICATOR_VALUE FROM COLLECTIONS WHERE ID = ?', (id,))
        collection = cur.fetchone()
        if collection == None:
            return api.abort(400, "No data found for given id")
        
        cur.execute('SELECT COUNTRY, VALUE FROM COUNTRIES WHERE COLLECTION_ID = ? AND DATE = ? ORDER BY VALUE DESC', (id, year, ))
        rows = cur.fetchall()
        num_rows = 0
        query = request.args.get('query')
        if not query is None:
            if query[0] == '-':
                num_rows = int(query[1:])
                rows.reverse()
            elif query[0] == '+':
                num_rows = int(query[1:])
            else:
                num_rows = int(query)
        rows = rows[:num_rows]
        if num_rows > 100:
            api.abort(400, "Please enter a query between 1 and 100")
        entries = []
        for row in rows:
            entries.append({
                "country": row[0],
                "value": row[1]
            })
        return jsonify({
            "indicator_value": collection[1], 
            "indicator": collection[0],
            "entries": entries
        })

DATABASE = './api.db'

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        c = db.cursor()

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='COLLECTIONS' ''')
        #if the count is 1, then table exists
        if not c.fetchone()[0]==1:
            c.execute('''CREATE TABLE COLLECTIONS ([ID] INTEGER PRIMARY KEY, [INDICATOR] Text, [CREATION_TIME] Date, [INDICATOR_VALUE] Text)''')
            c.execute('''CREATE TABLE COUNTRIES ([ID] INTEGER PRIMARY KEY, [COUNTRY] Text, [DATE] Text, [VALUE] NUMERIC, [COLLECTION_ID] INTEGER)''')
            db.commit()
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


if __name__ == '__main__':
    app.run(debug=True)

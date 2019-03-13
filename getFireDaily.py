"""
get fire data of modis and virrs daily
by Deyu.Tian
"""

from lxml import html,etree
import requests
import urllib.request as urllib_request
import psycopg2 as pypgsql
import sys
import csv
import ast


def getCSV():
    # url addr
    url = "https://firms.modaps.eosdis.nasa.gov/active_fire/#firms-txt"
    # request page
    page = requests.get(url)
    # get tree-text
    tree = html.fromstring(page.content)
    # get csv file path by element attributes
    csv_modis_24h = tree.xpath('//*[@id="active-fire-text"]/tbody/tr[13]/td[2]/a[1]/@href')[0]
    csv_virrs_24h = tree.xpath('//*[@id="active-fire-text"]/tbody/tr[13]/td[3]/a[1]/@href')[0]
    # split domain name
    domain_name = url.split('/')[2]
    # get full path of csv files 
    csv_modis_24h_url = "https://{}/{}".format(domain_name, csv_modis_24h)
    csv_virrs_24h_url = "https://{}/{}".format(domain_name, csv_virrs_24h)
    # now download data 
    urllib_request.urlretrieve(csv_modis_24h_url, csv_modis_24h.split('/')[-1])
    urllib_request.urlretrieve(csv_virrs_24h_url, csv_virrs_24h.split('/')[-1])
    # DATABASE PROCESS
    upsert_db()

def upsert_db():
    conn = None
    try:
        # new a connection object
        conn_str = "dbname = 'firstgis' user = 'postgres' host = 'localhost' password = 'admin123'"
        conn = pypgsql.connect(conn_str)
        print("CONNECT succeed")
        
        try:
            # new a query cursor
            cur = conn.cursor()
            temp_tb_sql = """
            CREATE TEMPORARY table temp_fire (
                Latitude	real,
                Longitude	real,
                Brightness	real,
                Scan	real,
                Track	real,
                Acq_Date	date,
                Acq_Time	time,
                Satellite	varchar(3),
                Confidence	varchar(10),
                _version	varchar(10),
                Bright_1	real,
                FRP		real,
                DayNight	varchar(3)
            );
            """
            cur.execute(temp_tb_sql)

            # get column names
            # csv_columns = ['oid', 'latitude', 'longitude', 'brightness', 'scan', 'track', 
            #     'acq_Date', 'acq_Time', 'satellite', 'confidence', '_version', 
            #     'bright_1', 'frp', 'daynight']
            cur.execute("Select * FROM temp_fire")
            colnames = [desc[0] for desc in cur.description]
            tmpstr = "{}".format(colnames)
            tmpstr = tmpstr.replace("'", "")
            # tmpstr = ast.literal_eval(tmpstr)
            print(tmpstr[1:-1])
            copy_sql = """COPY temp_fire ({}) FROM STDIN WITH CSV HEADER DELIMITER as ',';""".format(tmpstr[1:-1])
            print(copy_sql)
            try:
                with open('MODIS_C6_SouthEast_Asia_24h.csv', 'rb') as m, open('VNP14IMGTDL_NRT_SouthEast_Asia_24h.csv') as v:
                    cur.copy_expert(copy_sql, m)
                    cur.copy_expert(copy_sql, v)
            except:
                print("ERROR : copy csv to tmp database failed!")
            
            upsert_sql = """
            INSERT INTO firepoints
            SELECT *
            FROM temp_fire 
            ON CONFLICT (latitude, longitude, acq_date, acq_time) DO UPDATE SET
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                brightness = excluded.brightness,
                scan = excluded.scan,
                track = excluded.track,
                acq_date = excluded.acq_date,
                acq_time = excluded.acq_time,
                satellite = excluded.satellite,
                confidence = excluded.confidence,
                _version = excluded._version,
                bright_1 = excluded.bright_1,
                frp = excluded.frp,
                daynight = excluded.daynight;
            """
            print(upsert_sql)
            cur.execute(upsert_sql)
            conn.commit()
        except pypgsql.DatabaseError as e:
            if conn:
                conn.rollback()
            print('%s' % e)
    except pypgsql.DatabaseError as e:
        if conn:
            conn.rollback()
        print('Error is %s' % e)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    getCSV()


import psycopg2 as pypgsql
import os
import datetime


def export_day_shp(date_str):
    # 创建临时表
    create_day_geotb(date_str)
    filename = '/home/dorbodwolf/codes/python/fire/out_shp/{}'.format(date_str)
    # wenchang_geojson = '{\"type\": \"Polygon\", \"coordinates\": [ [ [ 110.4616049960662, 19.3545810164358 ], [ 111.0417989989043, 19.3545810164358 ], [ 111.0417989989043, 20.1611829993558 ], [ 110.4616049960662, 20.1611829993558 ], [ 110.4616049960662, 19.3545810164358 ] ] ] }'
    cmd = """
    /usr/bin/pgsql2shp -f {} -h localhost -p 5432 -u postgres -P 'admin123' firstgis  "SELECT * FROM geotb"
    """.format(filename)
    print(cmd)
    # 导出shp
    os.system(cmd)
    # 删除临时表
    drop_day_geodb()


def drop_day_geodb():
    conn = None
    try:
        # new a connection object
        conn_str = "dbname = 'firstgis' user = 'postgres' host = 'localhost' password = 'admin123'"
        conn = pypgsql.connect(conn_str)
        print("CONNECT succeed")
        try:
            # new a query cursor
            cur = conn.cursor()
            drop_geotb = """
                DROP TABLE geotb;
            """
            cur.execute(drop_geotb)
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

def create_day_geotb(date_str):
    conn = None
    try:
        # new a connection object
        conn_str = "dbname = 'firstgis' user = 'postgres' host = 'localhost' password = 'admin123'"
        conn = pypgsql.connect(conn_str)
        print("CONNECT succeed")
        try:
            # new a query cursor
            cur = conn.cursor()
            create_geotb = """
                CREATE TABLE IF NOT EXISTS geotb AS
                SELECT * FROM firepoints
                WHERE acq_date = '{}';
            """.format(date_str)
            cur.execute(create_geotb)
            add_update_geomcol = """
            alter table geotb add column if not exists geom geometry(Point, 4326);
            update geotb set geom=ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
            delete from geotb using wenchang  where NOT ST_Intersects(geotb.geom, wenchang.geom);
            """
            cur.execute(add_update_geomcol)
            # print(cur.fetchone())
            conn.commit()  #提交后才能在数据库中永久创建表，不然是临时表；python连接postgres数据库想要将更改持久化都要commit
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
    export_day_shp(datetime.datetime.today().strftime('%Y-%m-%d'))
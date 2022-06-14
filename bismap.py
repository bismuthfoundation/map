from celery import Celery
from datetime import datetime, timedelta
from flask import Flask, render_template
import geoip2.database
import os
import pathlib
import pandas as pd
import plotly.express as px
from plotly.offline import plot
from pycoingecko import CoinGeckoAPI
import sqlite3

bismap = Flask(__name__)
bismap.config['CELERY_BROKER_URL'] = os.environ['REDIS_URL']
bismap.config['CELERY_RESULT_BACKEND'] = os.environ['REDIS_URL']
bismap.config['SESSION_TYPE'] = 'filesystem'

celery = Celery(bismap.name, broker=bismap.config['CELERY_BROKER_URL'])
#celery.conf.update(app.config)
celery.config_from_object(bismap.config)


@bismap.before_first_request
def init():
    print("Initialising...")
    pathlib.Path('./bis_db').mkdir(parents=True, exist_ok=True)
    try:
        os.remove('./bis_db/bis.db')
    except:
        pass
    try:
        os.remove('UPDATING')
    except:
        pass
    con = sqlite3.connect('./bis_db/bis.db')
    con.execute('CREATE TABLE IF NOT EXISTS hypernode_map ('
                'date timestamp, '
                'city text, '
                'lat real, '
                'lon real, '
                'weight_total integer'
                ')')
    con.execute('CREATE TABLE IF NOT EXISTS hypernode_stats ('
                'date timestamp, '
                'bis_usd real, '
                'coin_supply integer, '
                'collateral integer, '
                'collateral_supply_perc real, '
                'roi real'                
                ')')
    print("Finished loading BIS db.")


def fetch_bis_data():
    print('Fetching BIS hypernode data')
    dfs = pd.read_html(io='https://hypernodes.bismuth.live/?page_id=163', header=0, match="Active Nodes")
    df = [x for x in dfs if len(x) > 0][0]

    bis_usd = None
    coin_supply = None
    collateral = None
    collateral_supply_perc = None
    roi = None
    try:
        coin_supply = int(df.iloc[-1][2])
        collateral = int(df.iloc[-2][7])
        collateral_supply_perc = collateral/coin_supply
        roi = df.iloc[-2][8].split('=')[-1]
    except Exception as e:
        print(e)
        print("Unable to fetch BIS stats")

    print("Fetching BIS USD price")
    try:
        cg = CoinGeckoAPI()
        fetched_data = cg.get_price(ids='bismuth', vs_currencies='usd')
        if type(fetched_data) is dict and 'bismuth' in fetched_data.keys() and 'usd' in fetched_data['bismuth'].keys():
            bis_usd = fetched_data['bismuth']['usd']
    except Exception as e:
        print("Unable to fetch BIS USD price")
        print(e)

    bis_stats = {"bis_usd": bis_usd,
                 "coin_supply": coin_supply,
                 "collateral": collateral,
                 "collateral_supply_perc": collateral_supply_perc,
                 "roi": roi}
    bis_stats = pd.DataFrame.from_records([bis_stats])
    print("BIS stats")
    print(bis_stats)
    df.drop(df.index[[-1, -2]], inplace=True)
    df["city"] = None
    df["lat"] = None
    df["lon"] = None
    print(f'${int(bis_stats.iloc[0]["bis_usd"] * bis_stats.iloc[0]["collateral"]):,}')
    print("Fetching geo info from hypernode IPs")
    with geoip2.database.Reader('./geo_db/GeoLite2-City.mmdb') as geo_reader:
        for row, index in df.iterrows():
            geo_data = geo_reader.city(df.loc[row]["IP"])
            df.at[row, "city"] = geo_data.city.name
            df.at[row, "lat"] = geo_data.location.latitude
            df.at[row, "lon"] = geo_data.location.longitude
    df.loc[df["city"].isna(), "city"] = ""
    df = pd.concat([*[df.loc[df["Weight"] == 1]]*1, *[df.loc[df["Weight"] == 2]]*2, *[df.loc[df["Weight"] == 3]]*3], ignore_index=True)
    df['lat_lon'] = df['lat'].astype(str) + "_" + df['lon'].astype(str)
    df_counts = df['lat_lon'].value_counts().to_frame()
    df_counts.reset_index(inplace=True)
    df_counts.columns = ['lat_lon', 'weight_total']
    df['weight_total'] = df.apply(lambda row: df_counts[(df_counts['lat_lon'] == row['lat_lon'])]['weight_total'].values[0], axis=1)
    df = df[["city", "lat", "lon", "weight_total"]]
    return df, bis_stats


def get_map_div(df):
    px.set_mapbox_access_token('$MAPBOX_TOKEN')
    fig_map = px.scatter_mapbox(df, lat="lat", lon="lon", hover_name="city",
                                hover_data={"city": False, "lat": False, "lon": False, "weight_total": True},
                                color_discrete_sequence=px.colors.qualitative.Set2,
                                size="weight_total", size_max=20, zoom=3)
    fig_map.update_layout(
        title="",
        mapbox_style='dark',
        autosize=True,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        #paper_bgcolor='#f0f0f0',
        legend={"yanchor": "top", "y": 0.99, "xanchor": "right", "x": 0.99},
        updatemenus=[
            dict(
                buttons=list([
                    dict(args=['mapbox.style', 'dark'], label='Dark', method='relayout'),
                    dict(args=['mapbox.style', 'light'], label='Light', method='relayout'),
                    dict(args=['mapbox.style', 'open-street-map'], label='Street', method='relayout'),
                    dict(args=['mapbox.style', 'satellite-streets'], label='Satellite', method='relayout'),
                ]),
                direction="down", pad={"r": 0, "t": 0}, showactive=True, bordercolor='#585858',
                x=0.01, xanchor="left", y=0.99, yanchor="top", font=dict(size=13)
                # bgcolor='#000000',
            )
        ]
    )
    plot_map_div = plot(fig_map, output_type='div', include_plotlyjs=True, config={'displayModeBar': False, 'editable': False})
    return plot_map_div


def update_db(input_df, input_stats):
    print("Updating database")
    this_dt = datetime.now()
    hypernode_map_update_ok = True
    hypernode_stats_update_ok = True
    # hypernode map data
    print(f'Rows in "hypernode_map" before update: {count_rows("hypernode_map")}')
    for index, row in input_df.iterrows():
        this_record = {"date": datetime.now(),
                       "city": row["city"],
                       "lat": row["lat"],
                       "lon": row["lon"],
                       "weight_total": row["weight_total"]}
        try:
            with sqlite3.connect('./bis_db/bis.db') as con:
                cur = con.cursor()
                cur.execute("INSERT INTO hypernode_map (date,city,lat,lon,weight_total) VALUES (?,?,?,?,?)",
                            (this_dt, row["city"], row["lat"], row["lon"], row["weight_total"]))
                con.commit()
        except Exception as e:
            print(e)
            print('unable to write map record')
            con.rollback()
            hypernode_map_update_ok = False
        finally:
            con.close()
    print(f'Rows in "hypernode_map" after update: {count_rows("hypernode_map")}')
    # hypernode stats
    input_stats = input_stats.iloc[0].to_dict()
    print(f'Rows in "hypernode_stats" before update: {count_rows("hypernode_stats")}')
    try:
        with sqlite3.connect('./bis_db/bis.db') as con:
            cur = con.cursor()
            cur.execute("INSERT INTO hypernode_stats (date,bis_usd,coin_supply,collateral,collateral_supply_perc,roi) VALUES (?,?,?,?,?,?)",
                        (this_dt,
                         input_stats["bis_usd"],
                         int(input_stats["coin_supply"]),
                         int(input_stats["collateral"]),
                         input_stats["collateral_supply_perc"],
                         input_stats["roi"]))
            con.commit()
    except Exception as e:
        print(e)
        print('unable to write stats record')
        con.rollback()
        hypernode_stats_update_ok = False
    finally:
        con.close()
    print(f'Rows in "hypernode_stats" after update: {count_rows("hypernode_stats")}')
    return hypernode_map_update_ok, hypernode_stats_update_ok


def last_update():
    print("Checking for most recent db records")
    last_update_map = None
    last_update_stats = None
    try:
        with sqlite3.connect('./bis_db/bis.db') as con:
            cur = con.cursor()
            query = cur.execute(
                "SELECT * FROM hypernode_map ORDER BY date DESC LIMIT 1;")
            colname = [d[0] for d in query.description]
            for row in query.fetchall():
                print(f'Last updated map record: {row[0]}')
                last_update_map = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
    except Exception as e:
        print(e)
        print('Unable to retrieve records from hypernode map table')

    try:
        with sqlite3.connect('./bis_db/bis.db') as con:
            cur = con.cursor()
            query = cur.execute(
                "SELECT * FROM hypernode_stats ORDER BY date DESC LIMIT 1;")
            colname = [d[0] for d in query.description]
            for row in query.fetchall():
                print(f'Last updated stat record: {row[0]}')
                last_update_stats = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
    except Exception as e:
        print(e)
        print('Unable to retrieve records from hypernode stats table')
    return last_update_map, last_update_stats


def count_rows(input_table):
    try:
        with sqlite3.connect('./bis_db/bis.db') as con:
            cur = con.cursor()
            query = cur.execute(
                f'SELECT * FROM {input_table};')
            return len(query.fetchall())
    except Exception as e:
        print(e)
        print(f'Unable to retrieve records from {input_table} table')
        return 0


def db_to_df(input_table, input_datetime):
    results = []
    try:
        with sqlite3.connect('./bis_db/bis.db', detect_types=sqlite3.PARSE_DECLTYPES) as con:
            cur = con.cursor()
            query = cur.execute(
                f'SELECT * FROM {input_table} WHERE date BETWEEN ? AND ?;', (input_datetime, input_datetime))
            colname = [d[0] for d in query.description]
            for row in query.fetchall():
                if input_table == 'hypernode_map':
                    record = {"city": row[1],
                              "lat": row[2],
                              "lon": row[3],
                              "weight_total": row[4]}
                    results.append(record)
                elif input_table == 'hypernode_stats':
                    record = {"bis_usd": row[1],
                              "coin_supply": row[2],
                              "collateral": row[3],
                              "collateral_supply_perc": row[4],
                              "roi": row[5]}
                    results.append(record)
    except Exception as e:
        print(e)
        print(f'Unable to retrieve records from {input_table} for {input_datetime}')
    return pd.DataFrame.from_records(results)


def delete_db_rows(input_table, input_datetime):
    print(f"Deleting DB rows from {input_table} {input_datetime}")
    #input_datetime = input_datetime - timedelta(seconds=1)
    print(f'Rows before delete: {count_rows(input_table)}')
    try:
        with sqlite3.connect('./bis_db/bis.db', detect_types=sqlite3.PARSE_DECLTYPES) as con:
            cur = con.cursor()
            cur.execute(
                f'DELETE FROM {input_table} WHERE date BETWEEN ? AND ?;', (input_datetime, input_datetime))
            con.commit()
    except Exception as e:
        print(e)
        print(f'Unable to DELETE records from {input_table} for {input_datetime}')
    print(f'Rows after delete: {count_rows(input_table)}')


@celery.task
def fetch_and_update(this_hypernode_map_last_update, this_hypernode_stats_last_update):
    print("Starting async task: fetch and update")
    print(os.getcwd())
    with open('/app/UPDATING', 'w+'):
        hypernode_map_df, hypernode_stats = fetch_bis_data()
        hypernode_map_update_ok, hypernode_stats_update_ok = update_db(hypernode_map_df, hypernode_stats)
        global hypernode_map_last_update
        global hypernode_stats_last_update
        hypernode_map_last_update, hypernode_stats_last_update = last_update()
        # cleanup previous records
        if hypernode_map_update_ok and this_hypernode_map_last_update is not None:
            delete_db_rows('hypernode_map', this_hypernode_map_last_update)
        if hypernode_stats_update_ok and this_hypernode_stats_last_update is not None:
            delete_db_rows('hypernode_stats', this_hypernode_stats_last_update)
        if this_hypernode_map_last_update is not None and this_hypernode_stats_last_update is not None:
            print(f'Updated db with {len(hypernode_map_df)} hypernode records')
            print(f'Updated db with new stats:')
            print(f'{hypernode_stats}, ${int(hypernode_stats.iloc[0]["bis_usd"]*hypernode_stats.iloc[0]["collateral"]):,}')
        print(f'Update times changed to {hypernode_map_last_update}, {hypernode_stats_last_update}')
        hypernode_map_last_update, hypernode_stats_last_update = last_update()
    try:
        os.remove('/app/UPDATING')
    except Exception as e:
        print(e)
        os.remove('/app/UPDATING')
    finally:
        print(os.getcwd())
        print('Unable to delete UPDATING file')
    print("Finished async task")


@bismap.route('/')
def index():
    hypernode_map_last_update, hypernode_stats_last_update = last_update()
    print(f'Last updated: {hypernode_map_last_update}, {hypernode_stats_last_update}')
    update_interval_seconds = 3600
    if count_rows("hypernode_map") == 0 and not os.path.isfile('/app/UPDATING'):  # first run
        print("Fetching INITIAL data")
        hypernode_map_df, hypernode_stats = fetch_bis_data()
        _, _ = update_db(hypernode_map_df, hypernode_stats)
    elif datetime.now()-timedelta(seconds=update_interval_seconds) > hypernode_map_last_update or datetime.now()-timedelta(seconds=update_interval_seconds) > hypernode_stats_last_update:
        if not os.path.isfile('/app/UPDATING'):
            print("Async: Updating database tables and deleting previous entries")
            task = fetch_and_update.apply_async(args=[hypernode_map_last_update, hypernode_stats_last_update], countdown=10)
            #fetch_and_update(hypernode_map_last_update, hypernode_stats_last_update)
        else:
            print("Not updating: task is already running")
    else:
        print(f'Not updating map data since last update was less than {update_interval_seconds} seconds ago:', hypernode_map_last_update)
        hypernode_map_df = db_to_df('hypernode_map', hypernode_map_last_update)
        hypernode_stats = db_to_df('hypernode_stats', hypernode_stats_last_update)

    hypernode_map_last_update, hypernode_stats_last_update = last_update()
    hypernode_map_df = db_to_df('hypernode_map', hypernode_map_last_update)
    hypernode_stats = db_to_df('hypernode_stats', hypernode_stats_last_update)

    stats = hypernode_stats.iloc[0]
    stats_message1 = f'Staking ${int(stats["bis_usd"]*stats["collateral"]):,} with {stats["roi"]} ROI'
    stats_message2 = f'{stats["collateral"]:,} BIS used as Hypernode collateral - {stats["collateral_supply_perc"]*100:.2f}% of the total supply.'
    this_map = get_map_div(hypernode_map_df)
    return render_template('mapbox_js.html', MAP_DIV=this_map, STATS_MESSAGE1=stats_message1, STATS_MESSAGE2=stats_message2)


if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    bismap.run(threaded=True, port=5000)

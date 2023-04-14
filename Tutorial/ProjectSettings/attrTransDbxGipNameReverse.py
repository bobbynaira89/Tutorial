"""
-- Transfers attribute from layerIn to layerOut based on similar line geometry
-- Switch layerIn to layerOut and vice versa
"""

import psycopg2, time, datetime, os, shutil
from platform import python_version
import datetime as dt
# import the plugins
from qgis.utils import plugins

# ==========================================================================================================
# ==========================================CHANGE CODE HERE ONLY ==========================================
# ==========================================================================================================
# db connection
db = 'oac'
schemaUse = 'aut_bev2022'

# layers
layerIn = 'ver_1100_strasse_l'
layerOut = 'linknetz_road_trans_name'

# values
buffValList = [20]
percentUse = 60
percentUseBtmLim = 50
percentUseBtmLim2 = 40

# value for filtering based on proximity
distTo = 30

# local folder
localDest = r"W:\BENUTZER\adebola.hassan\_230223"

# column info
colUse = 'gip_name'

# ==========================================================================================================
# ========================================== LEAVE CODE BELOW AS IT IS =====================================
# ==========================================================================================================

try:
    # start time
    if float(python_version()[:3]) <= 3.7:
        tic = time.clock()
    elif float(python_version()[:3]) > 3.7:
        tic = time.time()

    # start --------------------------------------------------------------------

    # create connection for layerOut
    connection = psycopg2.connect(user="mapper",
                                password="mapper",
                                host="192.168.80.45",
                                port="5432",
                                database=f"{db}")

    # create cursor for layerOut
    cursor = connection.cursor()

    # chekck if layer is valid
    layerInCheck = QgsVectorLayer(f"dbname='{db}' host=192.168.80.45 port=5432 user='mapper' sslmode=disable srid=3857 type=Polygon checkPrimaryKeyUnicity='1' table='{schemaUse}'.'{layerIn}' (geometry) sql=", 'layerInCheck', 'postgres')

    if layerInCheck.isValid():

        print(f'{layerIn} is valid.')
        print(f'Addition of necessary columns started.')
        # add necessary columns to layerOut
        queryJX = f"""
                    ALTER TABLE {schemaUse}.{layerOut}
                    DROP COLUMN IF EXISTS len_x,
                    ADD COLUMN IF NOT EXISTS len_x FLOAT,
                    DROP COLUMN IF EXISTS len_x_chk,
                    ADD COLUMN IF NOT EXISTS len_x_chk VARCHAR;

                    --update len_x
                    UPDATE {schemaUse}.{layerOut}
                    SET len_x = ST_Length(geometry);
                    """
        cursor.execute(queryJX)
        connection.commit()
        print(f'Addition of necessary columns finished.')

        print(f'Creation of tables to use started.')
        query = f"""
        DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x CASCADE;
        CREATE TABLE {schemaUse}.{layerIn}_x AS
        SELECT DISTINCT(a.*) FROM {schemaUse}.{layerIn} a, {schemaUse}.{layerOut} b
        WHERE ST_DWithin(a.geometry, b.geometry, {distTo})
        AND COALESCE(a.{colUse}, '') = '';

        -- create gist index
        DROP INDEX IF EXISTS {schemaUse}.{layerIn}_x_geom_idx;
        CREATE INDEX IF NOT EXISTS {layerIn}_x_geom_idx
        ON {schemaUse}.{layerIn}_x USING GIST (geometry);

        DROP TABLE IF EXISTS {schemaUse}.{layerOut}_x CASCADE;
        CREATE TABLE {schemaUse}.{layerOut}_x AS
        SELECT DISTINCT(b.*) FROM {schemaUse}.{layerIn}_x a, {schemaUse}.{layerOut} b
        WHERE ST_DWithin(a.geometry, b.geometry, {distTo});

        -- create gist index
        DROP INDEX IF EXISTS {schemaUse}.{layerOut}_x_geom_idx;
        CREATE INDEX IF NOT EXISTS {layerOut}_x_geom_idx
        ON {schemaUse}.{layerOut}_x USING GIST (geometry);

        """
        cursor.execute(query)
        connection.commit()
        print(f'Creation of tables to use finished.')

        for index, item in enumerate(buffValList):

            print(f'Processing for {layerOut}_{index} started.')

            query2 = f"""
            -- get dissolved buffer dump
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff CASCADE;
            CREATE TABLE {schemaUse}.{layerIn}_x_buff AS
            SELECT id, {colUse}, ST_Buffer(geometry, {buffValList[index]}) AS geometry
            FROM {schemaUse}.{layerIn}_x;

            -- create gist index
            DROP INDEX IF EXISTS {schemaUse}.{layerIn}_x_buff_geom_idx;
            CREATE INDEX IF NOT EXISTS {layerIn}_x_buff_geom_idx
            ON {schemaUse}.{layerIn}_x_buff USING GIST (geometry);

            -- get commonality
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff_common CASCADE;
            CREATE TABLE {schemaUse}.{layerIn}_x_buff_common AS
            SELECT b.id, a.len_x, a.{colUse}, ST_CollectionExtract(ST_Intersection(a.geometry, b.geometry), 2) geometry
            FROM {schemaUse}.{layerOut}_x a, {schemaUse}.{layerIn}_x_buff b
            WHERE ST_Intersects(a.geometry, b.geometry);

            ALTER TABLE {schemaUse}.{layerIn}_x_buff_common
            DROP COLUMN IF EXISTS len_max,
            ADD COLUMN len_max FLOAT;

            UPDATE {schemaUse}.{layerIn}_x_buff_common
            SET len_max = ST_Length(geometry);

            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff_common_max CASCADE;
            CREATE TABLE {schemaUse}.{layerIn}_x_buff_common_max AS
            SELECT DISTINCT ON (id) id, {colUse}, len_x, len_max, ((len_max * 100) / len_x) AS len_max_perc, geometry
            FROM {schemaUse}.{layerIn}_x_buff_common
            ORDER BY id, len_max DESC NULLS LAST;

            --update len_x_{index} column
            UPDATE {schemaUse}.{layerIn} a
            SET {colUse} = b.{colUse}
            FROM {schemaUse}.{layerIn}_x_buff_common_max b
            WHERE a.id = b.id
            AND b.len_max_perc >= {percentUse};

            UPDATE {schemaUse}.{layerIn} a
            SET {colUse} = b.{colUse},
            len_x_chk = 'Y'
            FROM {schemaUse}.{layerIn}_x_buff_common_max b
            WHERE a.id = b.id
            AND b.len_max_perc < {percentUse}
            AND b.len_max_perc >= {percentUseBtmLim};

            UPDATE {schemaUse}.{layerIn} a
            SET len_x_chk = 'YY'
            FROM {schemaUse}.{layerIn}_x_buff_common_max b
            WHERE a.id = b.id
            AND COALESCE(a.{colUse}, '') = ''
            AND b.len_max_perc < {percentUseBtmLim}
            AND b.len_max_perc >= {percentUseBtmLim2};

            UPDATE {schemaUse}.{layerIn}
            SET len_x_chk = NULL
            WHERE COALESCE({colUse}, '') != ''
            AND len_x_chk = 'YY';

            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerOut}_x CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff_common CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff_common_max CASCADE;
            """
            cursor.execute(query2)
            connection.commit()

            print(f'Processing for {layerOut}_{index} finished.')

        print(f'Table updates finished.')

    else:

        print(f'{layerIn} is invalid.')
        print(f'Addition of necessary columns stopped. Option could be to export {layerIn} to a local destination first before exporting to database')
    # end --------------------------------------------------------------------

    # finish time
    if float(python_version()[:3]) <= 3.7:
        toc = time.clock()
    elif float(python_version()[:3]) > 3.7:
        toc = time.time()
    #toc = time.clock()
    print('Program run time for processing is: ' + str(dt.timedelta(seconds=toc - tic)))

except Exception as e:
    print(e)

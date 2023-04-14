"""
-- Transfers attribute from layerIn to layerOut based on similar line geometry
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
layerIn = 'linknetz_road_trans_name'
layerOut = 'ver_1100_strasse_l'

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

    # load layer_in
    queryJN = f"""SELECT DISTINCT(ST_GeometryType(geometry)) FROM {schemaUse}.{layerIn} WHERE geometry IS NOT NULL;"""
    cursor.execute(queryJN)
    layerInGType = (cursor.fetchone()[0])[3:]

    layerInLines = QgsVectorLayer(f"dbname='{db}' host=192.168.80.45 port=5432 user='mapper' sslmode=disable srid=3857 type={layerInGType} checkPrimaryKeyUnicity='1' table='{schemaUse}'.'{layerIn}' (geometry) sql=", 'layerInLines', 'postgres')
    print(f'layer_in loaded.')

    # dissolve by field
    layerInLinesDiss = processing.run("native:dissolve", {"INPUT": layerInLines, "FIELD": colUse, "OUTPUT": "memory:"})['OUTPUT']
    print(f'First dissolve completed.')

    # buffer dissolve just a little
    layerInLinesDissBuff = processing.run("native:buffer", {"INPUT": layerInLinesDiss, "DISTANCE": 0.00001, "END_CAP_STYLE": 1, "OUTPUT": "memory:"})['OUTPUT']
    print(f'mini buffer created.')

    # make dissolve dump
    layerInLinesDissBuffDump = processing.run("native:multiparttosingleparts", {"INPUT": layerInLinesDissBuff, "OUTPUT": "memory:"})['OUTPUT']
    print(f'Buffer dump completed.')

    # drop id column
    layerInLinesDissBuffDumpDrop = processing.run("native:deletecolumn", {"INPUT": layerInLinesDissBuffDump, "COLUMN": 'id', "OUTPUT": f"memory:{layerIn}_bf"})['OUTPUT']
    print(f'id column dropped.')

    print(f'Dumped buffer ready for exporting to database.')

    """
    # export to db
    # path use

    outPathUse = localDest + '/' + f'{layerIn}_bf' + '.shp'

    # export to local
    QgsVectorFileWriter.writeAsVectorFormat(layerInLinesDissBuffDumpDrop,
                                            outPathUse,
                                            fileEncoding='utf-8',
                                            destCRS=layerInLinesDissBuffDumpDrop.dataProvider().crs(),
                                            driverName='ESRI Shapefile')

    print(f'{layerIn}_bf exported to local.')

    # load from local
    layerInLinesDissBuffDumpDrop = QgsVectorLayer(outPathUse, f'{layerIn}_bf', 'ogr')

    """
    processing.run("qgis:importintopostgis", {"INPUT": layerInLinesDissBuffDumpDrop,
                                              "DATABASE": f"45er_{db}",
                                              "SCHEMA": schemaUse, "GEOMETRY_COLUMN": 'geometry',
                                              "OVERWRITE": 'TRUE', "LOWERCASE_NAMES": 'TRUE',
                                              "OUTPUT": "memory:"})

    # reassign layer in
    layerIn = f"{layerIn}_bf"

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
        WHERE ST_DWithin(a.geometry, b.geometry, {distTo});

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
            SELECT {colUse}, ST_Buffer(geometry, {buffValList[index]}) AS geometry
            FROM {schemaUse}.{layerIn}_x;

            -- create gist index
            DROP INDEX IF EXISTS {schemaUse}.{layerIn}_x_buff_geom_idx;
            CREATE INDEX IF NOT EXISTS {layerIn}_x_buff_geom_idx
            ON {schemaUse}.{layerIn}_x_buff USING GIST (geometry);

            -- get commonality
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff_common CASCADE;
            CREATE TABLE {schemaUse}.{layerIn}_x_buff_common AS
            SELECT a.id, a.len_x, b.{colUse}, ST_CollectionExtract(ST_Intersection(a.geometry, b.geometry), 2) geometry
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
            UPDATE {schemaUse}.{layerOut} a
            SET {colUse} = b.{colUse}
            FROM {schemaUse}.{layerIn}_x_buff_common_max b
            WHERE a.id = b.id
            AND b.len_max_perc >= {percentUse};

            UPDATE {schemaUse}.{layerOut} a
            SET {colUse} = b.{colUse},
            len_x_chk = 'Y'
            FROM {schemaUse}.{layerIn}_x_buff_common_max b
            WHERE a.id = b.id
            AND b.len_max_perc < {percentUse}
            AND b.len_max_perc >= {percentUseBtmLim};

            UPDATE {schemaUse}.{layerOut} a
            SET len_x_chk = 'YY'
            FROM {schemaUse}.{layerIn}_x_buff_common_max b
            WHERE a.id = b.id
            AND COALESCE(a.{colUse}, '') = ''
            AND b.len_max_perc < {percentUseBtmLim}
            AND b.len_max_perc >= {percentUseBtmLim2};

            UPDATE {schemaUse}.{layerOut}
            SET len_x_chk = NULL
            WHERE COALESCE({colUse}, '') != ''
            AND len_x_chk = 'YY';

            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerOut}_x CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff_common CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerIn}_x_buff_common_max CASCADE;
            DROP TABLE IF EXISTS {schemaUse}.{layerIn} CASCADE;
            """
            cursor.execute(query2)
            connection.commit()

            print(f'Processing for {layerOut}_{index} finished.')

        print(f'Table updates finished.')

    else:

        print(f'{layerIn} is invalid.')
        print(f'Addition of necessary columns stopped. Option could be to export {layerIn} to a local destination first before exporting to database')
    # end --------------------------------------------------------------------

    ## string layer_out to layer_in to itself to avoid unwanted gaps at nodes that could be an issue during manual checks -- below added on 01/12/22

    # load layer_out 
    self.uri_db_out.setDataSource(self.road_schema_out, self.road_table_out, "geometry")
    layer_out = QgsVectorLayer(self.uri_db_out.uri(False), self.road_table_out, "postgres")
    if not layer_out.isValid():
        print("1st - layer_out not valid")
        self.append_logfile(codex, "1st - layer_out not valid for stringing to Ref.")
        raise BaseException('Process stopped due to invalid layer.')

    # finish time
    if float(python_version()[:3]) <= 3.7:
        toc = time.clock()
    elif float(python_version()[:3]) > 3.7:
        toc = time.time()
    #toc = time.clock()
    print('Program run time for processing is: ' + str(dt.timedelta(seconds=toc - tic)))

except Exception as e:
    print(e)

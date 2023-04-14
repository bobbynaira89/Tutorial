
    def doAttrTransfer(self):

        try:

            dirUse = ''
            dbname = 'x_test'
            schema = self.ui.schema_comboBox.currentText()
            table = self.ui.table_comboBox.currentText()
            schema2 = self.ui.schema_2_comboBox.currentText()
            tableToAdd = self.ui.featToAddTable_comboBox.currentText()
            time = datetime.now()
            #scriptName = f'{tester}2oac_attribute_transfer'

            #.createLogFileOther(dirUse, dbname, schema, table, scriptName, time, f'This will transfer feature attribute(s) from an old table to its corresponding features in a new table.')
            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)
            #.appendLogFileOther(dirUse, dbname, scriptName, time, f'Starting time for attribute transfer operation: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)
            #.appendLogFileOther(dirUse, dbname, scriptName, time, f'Parameters: feature buffer = {self.ui.featBuffer_lineEdit.text()}, network buffer = {self.ui.netBuffer_lineEdit.text()}, accuracy = {self.ui.accuracy_lineEdit.text()}%.')
            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)

            # db connect
            conn = psycopg2.connect(
                f"dbname = {dbname} port = {self.port} user = {self.user} host = {self.host} password = {self.password}")
            cur = conn.cursor()

            # --remove all current map layers in qgis interface [optional]
            QgsProject.instance().removeAllMapLayers()

            # --get distance object
            d = QgsDistanceArea()

            # --create folders to hold processed lines and points before export to database
            OutNonProLines = 'non_pro_lines'
            outOldDanglesCy = 'out_old_dangles_cy'
            outNewDanglesCy = 'out_new_dangles_cy'
            outOldDanglesMtb = 'out_old_dangles_mtb'
            outNewDanglesMtb = 'out_new_dangles_mtb'
            outOldDanglesHk = 'out_old_dangles_hk'
            outNewDanglesHk = 'out_new_dangles_hk'
            SwissProFilesFPth = f'{dirUse}/ProFiles'
            # create folder for files to be merged later
            if not os.path.exists(SwissProFilesFPth):
                os.makedirs(SwissProFilesFPth)
            else:
                shutil.rmtree(SwissProFilesFPth)
                os.makedirs(SwissProFilesFPth)
            
            # buffer values
            buffValue = float(self.ui.featBuffer_lineEdit.text())
            inBuffValue = (-buffValue * 2) / 3

            # ------------------------------------------ below added on 220113 ---------------------------------------------------------------------

            # add new column to main table to hold 'id' of old table (tableToAdd)
            queryCTX = f"""
                        ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS old_tab_id;
                        ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS old_tab_id INTEGER;
                        """
            cur.execute(queryCTX)
            conn.commit()

            # ------------------------------------------ above added on 220113 ---------------------------------------------------------------------

            # create a sub table from the old network table where values are present in the necessary fields to transfer from.
            #.appendLogFileOther(dirUse, dbname, scriptName, time, f'Creating table from old road network where there are sport values started at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            queryCT = f"""DROP TABLE IF EXISTS {schema}.{tableToAdd}_sub;
                            CREATE TABLE {schema}.{tableToAdd}_sub (LIKE {schema2}.{tableToAdd} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
                            INSERT INTO {schema}.{tableToAdd}_sub SELECT * FROM {schema2}.{tableToAdd}
                            WHERE COALESCE(name1, '') != '' OR COALESCE(name2, '') != '';"""
            cur.execute(queryCT)
            conn.commit()
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Creating table from old road network where there are sport values finished at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')

            # --load sub layer with values in the fields to transfer from.
            dbLayer = QgsVectorLayer(
                f"dbname='{dbname}' host={self.host} port={self.port} user='{self.user}' sslmode=disable srid=3857 type=LineString checkPrimaryKeyUnicity='1' table='{schema}'.'{tableToAdd}_sub' (geometry) sql=",
                'subLayer', 'postgres')
            dbLayerCount = dbLayer.featureCount()
            # QgsProject.instance().addMapLayer(dbLayer)
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Table created from old road network where there are sport values has {dbLayerCount} features.')

            # --create schema to hold check files
            useSchema = f'check_files_x'
            querySC = f"""DROP SCHEMA IF EXISTS {useSchema} CASCADE;
                            CREATE SCHEMA {useSchema};"""
            cur.execute(querySC)
            conn.commit()

            # ------------------------------------------ below added on 220114 -----------------------------------------------------------------

            # create in new schema a sub table from the new network table where values are present in the necessary fields to transfer from
            queryCX = f"""
                        DROP TABLE IF EXISTS {useSchema}.{table}_w_values_b4_pro;
                        CREATE TABLE {useSchema}.{table}_w_values_b4_pro (LIKE {schema}.{table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
                        INSERT INTO {useSchema}.{table}_w_values_b4_pro SELECT * FROM {schema}.{table}
                        WHERE COALESCE(sport, '')||COALESCE(num_cy, '')||COALESCE(num_mtb, '')||COALESCE(num_hk, '') != '';

                        DROP TABLE IF EXISTS {useSchema}.{table}_w_values_b4_pro_copy;
                        CREATE TABLE {useSchema}.{table}_w_values_b4_pro_copy (LIKE {schema}.{table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
                        INSERT INTO {useSchema}.{table}_w_values_b4_pro_copy SELECT * FROM {schema}.{table}
                        WHERE COALESCE(sport, '')||COALESCE(num_cy, '')||COALESCE(num_mtb, '')||COALESCE(num_hk, '') != '';
                        """
            cur.execute(queryCX)
            conn.commit()
            
            # create new activity columns with extension '_x sub table from the new network table where values are present in the necessary fields to transfer from.
            # These columns would hold newly updated values to be compared with already existing values.
            # If values are not the same at the end of the update, they are features of interest to be checked.
            newColumnList = ['name1_x', 'name2_x']
            for col in newColumnList:
                queryCXQ = f"""
                            ALTER TABLE {useSchema}.{table}_w_values_b4_pro DROP COLUMN IF EXISTS {col};
                            ALTER TABLE {useSchema}.{table}_w_values_b4_pro ADD COLUMN IF NOT EXISTS {col} VARCHAR;
                            """
                cur.execute(queryCXQ)
                conn.commit()
            # ------------------------------------------ below added on 220114 -----------------------------------------------------------------

            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'New table updates started at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')

            queryCXV = f"""

                        UPDATE {schema}.{table} a
                        SET sport = b.sport,
                        name1 = b.name1,
                        name2 = b.name2
                        old_tab_id = b.id
                        FROM {schema}.{tableToAdd}_sub b
                        WHERE ST_Within(a.geometry, ST_Buffer(b.geometry, {buffValue}));

                        """
            cur.execute(queryCXV)
            conn.commit()

            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'New table updates finished at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')

            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)

            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Creation of non-processed features started at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')


            # create non_prolines using ids updated in old_tab_id
            queryDD = f"""
                        -- create non_prolines using ids updated in old_tab_id
                        DROP TABLE IF EXISTS {useSchema}.non_prolines;
                        CREATE TABLE {useSchema}.non_prolines AS
                        SELECT * 
                        FROM  {schema}.{tableToAdd}_sub
                        WHERE id NOT IN (SELECT old_tab_id FROM {schema}.{table} WHERE old_tab_id IS NOT NULL);

                        -- get non_prolines2 where old feature intersects new feature but new feature(s) were not updated
                        ALTER TABLE {schema}.{tableToAdd}_sub DROP COLUMN IF EXISTS buff_geom;
                        ALTER TABLE {schema}.{tableToAdd}_sub ADD COLUMN IF NOT EXISTS buff_geom geometry;

                        -- using buffer out and buffer in values
                        UPDATE {schema}.{tableToAdd}_sub 
                        SET buff_geom = ST_Buffer(ST_Buffer(geometry, {buffValue}, 'endcap=flat'), {inBuffValue}, 'endcap=flat');

                        -- get non_prolines2
                        DROP TABLE IF EXISTS {useSchema}.non_prolines2;
                        CREATE TABLE {useSchema}.non_prolines2 AS
                        SELECT a.* 
                        FROM  {schema}.{tableToAdd}_sub a, {schema}.{table} b
                        WHERE ST_Intersects(a.buff_geom, b.geometry)
                        AND b.old_tab_id IS NULL
                        AND COALESCE(b.btu) = '';

                        -- delete from non_prolines where already in non_prolines2
                        DELETE FROM {useSchema}.non_prolines2 WHERE id IN (SELECT id FROM {useSchema}.non_prolines);

                        -- drop buff_geom column in non_prolines2 to allow for easy insert of features into non_prolines
                        ALTER TABLE {useSchema}.non_prolines2 DROP COLUMN IF EXISTS buff_geom;

                        INSERT INTO {useSchema}.non_prolines
                        SELECT * FROM {useSchema}.non_prolines2;

                        -------------------------------------------- below added on 220207 ---------------------------------------------

                        -- get partially_pro_1
                        DROP TABLE IF EXISTS {useSchema}.partially_pro_1;
                        CREATE TABLE {useSchema}.partially_pro_1 AS
                        SELECT old_tab_id, SUM(ST_Length(geometry)) AS sum_len
                        FROM {schema}.{table}
                        WHERE old_tab_id IS NOT NULL
                        AND old_tab_id NOT IN (SELECT id FROM {useSchema}.non_prolines)
                        GROUP BY old_tab_id;

                        """
            cur.execute(queryDD)
            conn.commit()

            # cause 2 mins sleep before next heavy computation
            time.sleep(120)

            queryDD2 = f"""

                        -- get partially_pro_2 
                        DROP TABLE IF EXISTS {useSchema}.partially_pro_2;
                        CREATE TABLE {useSchema}.partially_pro_2 AS
                        SELECT a.id, b.old_tab_id, ST_Length(a.geometry) AS sum_lenx, b.sum_len, 0.0 AS perc
                        FROM {schema}.{tableToAdd}_sub a, {useSchema}.partially_pro_1 b
                        WHERE a.id = b.old_tab_id;

                        """
            cur.execute(queryDD2)
            conn.commit()

            # cause 30 secs sleep after heavy computation
            time.sleep(30)

            queryDD3 = f"""

                        UPDATE {useSchema}.partially_pro_2
                        SET perc = (sum_len * 100) / sum_lenx;

                        -- delete from partially_pro_2 where percentage is < 95
                        DELETE FROM {useSchema}.partially_pro_2
                        WHERE perc > 95;

                        -- delete from partially_pro_2 if already in non_prolines # -- done already in partially_pro_1 creation. Done again just in case
                        DELETE FROM {useSchema}.partially_pro_2
                        WHERE id IN (SELECT id FROM {useSchema}.non_prolines);

                        -- get partially_pro_3 as geometric table since 1 and 2 are both non-geometric tables
                        DROP TABLE IF EXISTS {useSchema}.partially_pro_3;
                        CREATE TABLE {useSchema}.partially_pro_3 AS
                        SELECT * 
                        FROM  {schema}.{tableToAdd}_sub
                        WHERE id IN (SELECT id FROM {useSchema}.partially_pro_2);

                        -- drop buff_geom column in partially_pro_2 to allow for easy insert of features into non_prolines
                        ALTER TABLE {useSchema}.partially_pro_3 DROP COLUMN IF EXISTS buff_geom;

                        INSERT INTO {useSchema}.non_prolines
                        SELECT * FROM {useSchema}.partially_pro_3;

                        DROP TABLE IF EXISTS {useSchema}.partially_pro_1;
                        DROP TABLE IF EXISTS {useSchema}.partially_pro_2;
                        DROP TABLE IF EXISTS {useSchema}.partially_pro_3;

                        -------------------------------------------- above added on 220207 ---------------------------------------------


                        -- create new table to hold unique values of non_prolines
                        DROP TABLE IF EXISTS {useSchema}.non_prolines_use;
                        CREATE TABLE {useSchema}.non_prolines_use AS
                        SELECT DISTINCT ON (id) * FROM {useSchema}.non_prolines;

                        -- drop tables
                        DROP TABLE IF EXISTS {useSchema}.non_prolines;
                        DROP TABLE IF EXISTS {useSchema}.non_prolines2;

                        -- update extension '_x' activity columns with newly updated values
                        UPDATE {useSchema}.{table}_w_values_b4_pro a
                        SET sport_x = b.sport,
                        num_cy_x = b.num_cy,
                        num_mtb_x = b.num_mtb,
                        num_hk_x = b.num_hk
                        FROM {schema}.{table} b
                        WHERE a.id = b.id;

                        -- delete features where activity columns and '_x' columns are the same. Undeleted features are features of interest to be checked as to the value disparity
                        DELETE FROM {useSchema}.{table}_w_values_b4_pro
                        WHERE COALESCE(sport, '')||COALESCE(num_cy, '')||COALESCE(num_mtb, '')||COALESCE(num_hk, '') = 
                        COALESCE(sport_x, '')||COALESCE(num_cy_x, '')||COALESCE(num_mtb_x, '')||COALESCE(num_hk_x, '');

                        -- rename tables
                        ALTER TABLE {useSchema}.{table}_w_values_b4_pro RENAME TO {table}_w_values_after_pro;
                        ALTER TABLE {useSchema}.{table}_w_values_b4_pro_copy RENAME TO {table}_w_values_b4_pro;

                        -- new tables to reprocess using activity tool
                        DROP TABLE IF EXISTS {useSchema}.to_pro_hk;
                        CREATE TABLE {useSchema}.to_pro_hk AS
                        SELECT id, sport AS a_sport, num_hk AS a_num_hk, geometry
                        FROM {useSchema}.non_prolines_use
                        WHERE COALESCE(num_hk, '') != '' OR COALESCE(sport, '') LIKE '%HK01%' OR COALESCE(sport, '') LIKE '%HK02%';

                        UPDATE {useSchema}.to_pro_hk
                        SET a_sport = (
                                CASE 
                                WHEN COALESCE(a_sport, '') LIKE '%HK02%' THEN '+HK01+HK02+'
                                WHEN (COALESCE(a_sport, '') LIKE '%HK01%' AND COALESCE(a_sport, '') NOT LIKE '%HK02%') THEN '+HK01+' 
                                END);

                        DROP TABLE IF EXISTS {useSchema}.to_pro_cy;
                        CREATE TABLE {useSchema}.to_pro_cy AS
                        SELECT id, sport AS a_sport, num_cy AS a_num_cy, geometry
                        FROM {useSchema}.non_prolines_use
                        WHERE COALESCE(num_cy, '') != '' OR COALESCE(sport, '') LIKE '%CY01%' OR COALESCE(sport, '') LIKE '%CY02%';

                        UPDATE {useSchema}.to_pro_cy
                        SET a_sport = (
                                CASE 
                                WHEN COALESCE(a_sport, '') LIKE '%CY02%' THEN '+CY01+CY02+'
                                WHEN (COALESCE(a_sport, '') LIKE '%CY01%' AND COALESCE(a_sport, '') NOT LIKE '%CY02%') THEN '+CY01+' 
                                END);

                        DROP TABLE IF EXISTS {useSchema}.to_pro_mtb;
                        CREATE TABLE {useSchema}.to_pro_mtb AS
                        SELECT id, sport AS a_sport, num_mtb AS a_num_mtb, geometry
                        FROM {useSchema}.non_prolines_use
                        WHERE COALESCE(num_mtb, '') != '' OR COALESCE(sport, '') LIKE '%CY03%' OR COALESCE(sport, '') LIKE '%CY04%';

                        UPDATE {useSchema}.to_pro_mtb
                        SET a_sport = (
                                CASE 
                                WHEN COALESCE(a_sport, '') LIKE '%CY04%' THEN '+CY03+CY04+'
                                WHEN (COALESCE(a_sport, '') LIKE '%CY03%' AND COALESCE(a_sport, '') NOT LIKE '%CY04%') THEN '+CY03+' 
                                END);

                        --DROP TABLE IF EXISTS {useSchema}.to_pro_sport;
                        --CREATE TABLE {useSchema}.to_pro_sport AS
                        --SELECT id, sport AS a_sport, num_hk AS a_num_hk, num_cy AS a_num_cy, num_mtb AS a_num_mtb, geometry
                        --FROM {useSchema}.non_prolines_use
                        --WHERE COALESCE(sport, '') != '' AND COALESCE(num_cy, '')||COALESCE(num_mtb, '')||COALESCE(num_hk, '') = '';

                        """
            cur.execute(queryDD3)
            conn.commit()

            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Creation of non-processed features finished at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            # ------------------------------------------ above added on 220113 ---------------------------------------------------------------------
            
            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)

            # create copy of tables. This needs to be done in order to alter the geometry column in view of working with ST_StartPoint and ST_EndPoint functions.
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Making table copies started at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            queryNT1 = f"""
                        DROP TABLE IF EXISTS {schema}.{table}_copy;
                        CREATE TABLE {schema}.{table}_copy (LIKE {schema}.{table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
                        INSERT INTO {schema}.{table}_copy SELECT * FROM {schema}.{table}
                        WHERE COALESCE(sport, '')||COALESCE(num_cy, '')||COALESCE(num_mtb, '')||COALESCE(num_hk, '') != '';
                            
                        -- create extra check table where old feature's buffer contains new feature but id not passed into old_tab_id column in the new features table...
                        -- This could be due to new feature being very short and hence falling into two or more differnt buffers of old features causing some ids in old_tab_id...
                        -- to be overwritten.
                        ALTER TABLE {schema}.{table}_copy DROP COLUMN IF EXISTS int_cnt_x;
                        ALTER TABLE {schema}.{table}_copy ADD COLUMN IF NOT EXISTS int_cnt_x integer;

                        UPDATE {schema}.{table}_copy a
                        SET int_cnt_x = COALESCE(a.int_cnt_x, 0) + 1
                        FROM {schema}.{table}_copy b
                        WHERE ST_Within(a.geometry, ST_Buffer(b.geometry, {buffValue}))
                        AND a.id != b.id;

                        DROP TABLE IF EXISTS {useSchema}.extra_check;
                        CREATE TABLE {useSchema}.extra_check AS
                        SELECT * FROM {schema}.{table}_copy WHERE int_cnt_x > 0;

                        """
            cur.execute(queryNT1)
            conn.commit()

            queryNT2 = f"""DROP TABLE IF EXISTS {schema}.{tableToAdd}_sub_copy;
                            CREATE TABLE {schema}.{tableToAdd}_sub_copy (LIKE {schema}.{tableToAdd}_sub INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
                            INSERT INTO {schema}.{tableToAdd}_sub_copy SELECT * FROM {schema}.{tableToAdd}_sub;"""
            cur.execute(queryNT2)
            conn.commit()
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Making table copies finished at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')

            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Making tables for dangle points for different activities started at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            # alter geometry column for both tables created above in order to use postgis functions on multiline string
            queryAG = f"""ALTER TABLE {schema}.{table}_copy ALTER COLUMN geometry TYPE geometry(linestring,3857) USING ST_GeometryN(ST_Force2D(geometry), 1);
                            ALTER TABLE {schema}.{tableToAdd}_sub_copy ALTER COLUMN geometry TYPE geometry(linestring,3857) USING ST_GeometryN(ST_Force2D(geometry), 1);"""
            cur.execute(queryAG)
            conn.commit()

            # --generate dangles for %CY01%
            # old
            queryNE = f"""
                            DROP TABLE IF EXISTS {useSchema}.{outOldDanglesCy};
                            CREATE TABLE {useSchema}.{outOldDanglesCy} AS
                            SELECT  ST_StartPoint(geometry) AS geometry FROM {schema}.{tableToAdd}_sub_copy WHERE sport LIKE '%CY01%' OR sport LIKE '%CY02%'
                            UNION ALL
                            SELECT  ST_EndPoint(geometry) AS geometry FROM {schema}.{tableToAdd}_sub_copy WHERE sport LIKE '%CY01%' OR sport LIKE '%CY02%';
                            """
            cur.execute(queryNE)
            conn.commit()
            # new
            queryOL = f"""
                            DROP TABLE IF EXISTS {useSchema}.{outNewDanglesCy};
                            CREATE TABLE {useSchema}.{outNewDanglesCy} AS
                            SELECT  ST_StartPoint(geometry) AS geometry FROM {schema}.{table}_copy WHERE sport LIKE '%CY01%' OR sport LIKE '%CY02%'
                            UNION ALL
                            SELECT  ST_EndPoint(geometry) AS geometry FROM {schema}.{table}_copy WHERE sport LIKE '%CY01%' OR sport LIKE '%CY02%';
                            """
            cur.execute(queryOL)
            conn.commit()

            # generate dangles for %CY03%
            # old
            queryNE = f"""
                            DROP TABLE IF EXISTS {useSchema}.{outOldDanglesMtb};
                            CREATE TABLE {useSchema}.{outOldDanglesMtb} AS
                            SELECT  ST_StartPoint(geometry) AS geometry FROM {schema}.{tableToAdd}_sub_copy WHERE sport LIKE '%CY03%' OR sport LIKE '%CY04%'
                            UNION ALL
                            SELECT  ST_EndPoint(geometry) AS geometry FROM {schema}.{tableToAdd}_sub_copy WHERE sport LIKE '%CY03%' OR sport LIKE '%CY04%';
                            """
            cur.execute(queryNE)
            conn.commit()
            # new
            queryOL = f"""
                            DROP TABLE IF EXISTS {useSchema}.{outNewDanglesMtb};
                            CREATE TABLE {useSchema}.{outNewDanglesMtb} AS
                            SELECT  ST_StartPoint(geometry) AS geometry FROM {schema}.{table}_copy WHERE sport LIKE '%CY03%' OR sport LIKE '%CY04%'
                            UNION ALL
                            SELECT  ST_EndPoint(geometry) AS geometry FROM {schema}.{table}_copy WHERE sport LIKE '%CY03%' OR sport LIKE '%CY04%';
                            """
            cur.execute(queryOL)
            conn.commit()

            # generate dangles for %HK01%
            # old
            queryNE = f"""
                            DROP TABLE IF EXISTS {useSchema}.{outOldDanglesHk};
                            CREATE TABLE {useSchema}.{outOldDanglesHk} AS
                            SELECT  ST_StartPoint(geometry) AS geometry FROM {schema}.{tableToAdd}_sub_copy WHERE sport LIKE '%HK01%' OR sport LIKE '%HK02%'
                            UNION ALL
                            SELECT  ST_EndPoint(geometry) AS geometry FROM {schema}.{tableToAdd}_sub_copy WHERE sport LIKE '%HK01%' OR sport LIKE '%HK02%';
                            """
            cur.execute(queryNE)
            conn.commit()
            # new
            queryOL = f"""
                            DROP TABLE IF EXISTS {useSchema}.{outNewDanglesHk};
                            CREATE TABLE {useSchema}.{outNewDanglesHk} AS
                            SELECT  ST_StartPoint(geometry) AS geometry FROM {schema}.{table}_copy WHERE sport LIKE '%HK01%' OR sport LIKE '%HK02%'
                            UNION ALL
                            SELECT  ST_EndPoint(geometry) AS geometry FROM {schema}.{table}_copy WHERE sport LIKE '%HK01%' OR sport LIKE '%HK02%';
                            """
            cur.execute(queryOL)
            conn.commit()
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Making tables for dangle points for different activities finished at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')

            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Making table with resolved dangle points based on proximity for different activities started at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            # remove points in proximity
            newTabList = [outNewDanglesCy, outNewDanglesMtb, outNewDanglesHk]
            oldTabList = [outOldDanglesCy, outOldDanglesMtb, outOldDanglesHk]
            cleanTabList = ['out_cy_cleaned', 'out_mtb_cleaned', 'out_hk_cleaned']

            for newTab, oldTab, cleanTab in zip(newTabList, oldTabList, cleanTabList):
                # create merge table of old and new points
                queryMT = f"""
                            -- make copies
                            DROP TABLE IF EXISTS {useSchema}.{oldTab}_copy;
                            CREATE TABLE {useSchema}.{oldTab}_copy AS TABLE {useSchema}.{oldTab};

                            DROP TABLE IF EXISTS {useSchema}.{newTab}_copy;
                            CREATE TABLE {useSchema}.{newTab}_copy AS TABLE {useSchema}.{newTab};

                            -- delete in new table if in old table and vice versa
                            DELETE FROM {useSchema}.{newTab}
                            WHERE geometry::VARCHAR IN (SELECT geometry::VARCHAR FROM {useSchema}.{oldTab}_copy);

                            DELETE FROM {useSchema}.{oldTab}
                            WHERE geometry::VARCHAR IN (SELECT geometry::VARCHAR FROM {useSchema}.{newTab}_copy);

                            -- add id column
                            ALTER TABLE {useSchema}.{newTab} 
                            ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY;

                            ALTER TABLE {useSchema}.{oldTab} 
                            ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY;

                            -- drop tables
                            DROP TABLE IF EXISTS {useSchema}.{oldTab}_copy;
                            DROP TABLE IF EXISTS {useSchema}.{newTab}_copy;

                            -- create clean table
                            DROP TABLE IF EXISTS {useSchema}.{cleanTab};
                            CREATE TABLE  {useSchema}.{cleanTab} AS
                            SELECT geometry, 'New Feature' AS source_ref FROM  {useSchema}.{newTab}
                            UNION ALL
                            SELECT geometry, 'Old Feature' AS source_ref FROM  {useSchema}.{oldTab};

                            ALTER TABLE {useSchema}.{cleanTab} 
                            ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY,
                            ADD COLUMN IF NOT EXISTS dist FLOAT;
                            
                            """
                cur.execute(queryMT)
                conn.commit()

                # --------------------------------------- below commented out on 220117 -----------------------------------------------------------
                # # update minimum distance values
                # queryMD = f"""UPDATE {useSchema}.{cleanTab} t1
                #                 SET dist = (SELECT ST_Distance(t1.geometry, t2.geometry) FROM {useSchema}.{cleanTab} t2 
                #                 WHERE t2.id <> t1.id ORDER BY ST_Distance(t1.geometry, t2.geometry) LIMIT 1);"""
                # cur.execute(queryMD)
                # conn.commit()

                # # delete points within a proximity. Value 0.2m works well
                # queryDP = f"""DELETE FROM {useSchema}.{cleanTab}
                #                 WHERE dist < {buffValue};"""
                # cur.execute(queryDP)
                # conn.commit()
                # --------------------------------------- above commented out on 220117 -----------------------------------------------------------

            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Making table with resolved dangle points based on proximity for different activities finished at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')

            # drop not necessary tables
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Deleting unnecessary tables started at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            queryDT = f"""DROP TABLE IF EXISTS {schema}.{table}_copy;
                            DROP TABLE IF EXISTS {schema}.{tableToAdd}_sub;
                            DROP TABLE IF EXISTS {schema}.{tableToAdd}_sub_copy;"""
            cur.execute(queryDT)
            conn.commit()
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Deleting unnecessary tables finished at: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')

            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)
            #.appendLogFileOther(dirUse, dbname, scriptName, time,
                                             f'Ending time for attribute transfer operation: {datetime.now().strftime("%y%m%d_%H:%M:%S")}')
            #.appendLogFileOther(dirUse, dbname, scriptName, time, '-' * 100)

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table", error)
        finally:
            # closing database connection.
            if (conn):
                cur.close()
                conn.close()
                print("PostgreSQL connection is closed")
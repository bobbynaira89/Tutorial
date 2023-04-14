
-- preprocessings
ALTER TABLE swisstopo_cycling_flat_l
DROP COLUMN IF EXISTS name_x,
DROP COLUMN IF EXISTS oa_colour_x;

ALTER TABLE swisstopo_cycling_flat_l
ADD COLUMN IF NOT EXISTS name_x VARCHAR,
ADD COLUMN IF NOT EXISTS oa_colour_x VARCHAR;

-- update name_x
UPDATE swisstopo_cycling_flat_l
SET name_x = REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(name
	,'^(K|k|P|p|S|s|l|L|Z|z|♥|t)(\+|■|\,|\-|L|↺|c|C|▲|Ω|▙|3|q|x|X|II|b|4|\s|●|O|T|templom|kör|kápolna|négyzet)(\s?|\,?|kör?)(\(|\,?|\+?)?',''),
															'\(',''),'\)',''),'^(\-|●)',''),'^\s','');

-- update oa_colour_x
UPDATE swisstopo_cycling_flat_l
SET oa_colour_x = (CASE
          					WHEN oa_colour = 'blue' THEN 'B'
          					WHEN oa_colour = 'red' THEN 'A'
          					WHEN oa_colour = 'yellow' THEN 'D'
          					WHEN oa_colour = 'green' THEN 'C'
          					WHEN oa_colour = 'purple' THEN 'E'
          					ELSE 'F'
          				END);

-- create new table
DROP TABLE IF EXISTS swisstopo_cycling_flat_l_v2 CASCADE;
CREATE TABLE swisstopo_cycling_flat_l_v2 AS
WITH root_tab AS (
SELECT DISTINCT ON (id,geometry) id,geometry,oa_source,type,tracktype,oa_country
FROM swisstopo_cycling_flat_l),

ordered_tab AS (
  SELECT * FROM swisstopo_cycling_flat_l ORDER BY id
),

-- swisstopo_id casted as varchar
swisstopo_id_agg AS
(SELECT id, STRING_TO_ARRAY(agg, '$') agg_use FROM
(SELECT id, STRING_AGG(COALESCE(swisstopo_id::VARCHAR, ''), '$') agg
FROM ordered_tab
GROUP BY id) foo),

name_x_agg AS
(
	SELECT id, STRING_TO_ARRAY(name_x, '               ') agg_use
	FROM ordered_tab
),
--(SELECT id, STRING_TO_ARRAY(agg, '$') agg_use FROM
--(SELECT id, STRING_AGG(COALESCE(name_x, ''), '$') agg
--FROM ordered_tab
--GROUP BY id) foo),

ref_agg AS
(SELECT id, STRING_TO_ARRAY(agg, '$') agg_use FROM
(SELECT id, STRING_AGG(COALESCE(ref, ''), '$') agg
FROM ordered_tab
GROUP BY id) foo),

network_agg AS
(SELECT id, STRING_TO_ARRAY(agg, '$') agg_use FROM
(SELECT id, STRING_AGG(COALESCE(network, ''), '$') agg
FROM ordered_tab
GROUP BY id) foo),

oa_colour_x_agg AS
(SELECT id, STRING_TO_ARRAY(agg, '$') agg_use FROM
(SELECT id, STRING_AGG(COALESCE(oa_colour_x, ''), '$') agg
FROM ordered_tab
GROUP BY id) foo),

oa_icon_agg AS
(SELECT id, STRING_TO_ARRAY(agg, '$') agg_use FROM
(SELECT id, STRING_AGG(COALESCE(oa_icon, ''), '$') agg
FROM ordered_tab
GROUP BY id) foo),

----------------- start joins to root_tab
join_1 AS
(SELECT a.*, b.agg_use swisstopo_id_aggx FROM root_tab a LEFT OUTER JOIN swisstopo_id_agg b ON a.id = b.id),

join_2 AS
(SELECT a.*, b.agg_use name_x_aggx FROM join_1 a LEFT OUTER JOIN name_x_agg b ON a.id = b.id),

join_3 AS
(SELECT a.*, b.agg_use ref_aggx FROM join_2 a LEFT OUTER JOIN ref_agg b ON a.id = b.id),

join_4 AS
(SELECT a.*, b.agg_use network_aggx FROM join_3 a LEFT OUTER JOIN network_agg b ON a.id = b.id),

join_5 AS
(SELECT a.*, b.agg_use oa_colour_x_aggx FROM join_4 a LEFT OUTER JOIN oa_colour_x_agg b ON a.id = b.id),

join_6 AS
(SELECT a.*, b.agg_use oa_icon_aggx FROM join_5 a LEFT OUTER JOIN oa_icon_agg b ON a.id = b.id)

-- select for new table
SELECT id,geometry,oa_source,type,tracktype,oa_country,

-- cast swisstopo_id back to integer
swisstopo_id_aggx,
swisstopo_id_aggx[1]::BIGINT swisstopo_id_a,
swisstopo_id_aggx[2]::BIGINT swisstopo_id_b,
swisstopo_id_aggx[3]::BIGINT swisstopo_id_c,
swisstopo_id_aggx[4]::BIGINT swisstopo_id_d,
swisstopo_id_aggx[5]::BIGINT swisstopo_id_e,
swisstopo_id_aggx[6]::BIGINT swisstopo_id_f,
(SELECT ARRAY_LENGTH(swisstopo_id_aggx, 1) swisstopo_id_len),
(SELECT COUNT(*) swisstopo_id_valid FROM UNNEST(swisstopo_id_aggx) swisstopo_id_valid WHERE COALESCE(swisstopo_id_valid) != ''),

name_x_aggx name_aggx,
name_x_aggx[1] name_a,
name_x_aggx[2] name_b,
name_x_aggx[3] name_c,
name_x_aggx[4] name_d,
name_x_aggx[5] name_e,
name_x_aggx[6] name_f,
(SELECT ARRAY_LENGTH(name_x_aggx, 1) name_len),
(SELECT COUNT(*) name_valid FROM UNNEST(name_x_aggx) name_valid WHERE COALESCE(name_valid) != ''),

ref_aggx,
ref_aggx[1] ref_a,
ref_aggx[2] ref_b,
ref_aggx[3] ref_c,
ref_aggx[4] ref_d,
ref_aggx[5] ref_e,
ref_aggx[6] ref_f,
(SELECT ARRAY_LENGTH(ref_aggx, 1) ref_len),
(SELECT COUNT(*) ref_valid FROM UNNEST(ref_aggx) ref_valid WHERE COALESCE(ref_valid) != ''),

network_aggx,
network_aggx[1] network_a,
network_aggx[2] network_b,
network_aggx[3] network_c,
network_aggx[4] network_d,
network_aggx[5] network_e,
network_aggx[6] network_f,
(SELECT ARRAY_LENGTH(network_aggx, 1) network_len),
(SELECT COUNT(*) network_valid FROM UNNEST(network_aggx) network_valid WHERE COALESCE(network_valid) != ''),

oa_colour_x_aggx oa_colour_aggx,
oa_colour_x_aggx[1] oa_colour_a,
oa_colour_x_aggx[2] oa_colour_b,
oa_colour_x_aggx[3] oa_colour_c,
oa_colour_x_aggx[4] oa_colour_d,
oa_colour_x_aggx[5] oa_colour_e,
oa_colour_x_aggx[6] oa_colour_f,
(SELECT ARRAY_LENGTH(oa_colour_x_aggx, 1) oa_colour_len),
(SELECT COUNT(*) oa_colour_valid FROM UNNEST(oa_colour_x_aggx) oa_colour_valid WHERE COALESCE(oa_colour_valid) != ''),

oa_icon_aggx,
oa_icon_aggx[1] oa_icon_a,
oa_icon_aggx[2] oa_icon_b,
oa_icon_aggx[3] oa_icon_c,
oa_icon_aggx[4] oa_icon_d,
oa_icon_aggx[5] oa_icon_e,
oa_icon_aggx[6] oa_icon_f,
(SELECT ARRAY_LENGTH(oa_icon_aggx, 1) oa_icon_len),
(SELECT COUNT(*) oa_icon_valid FROM UNNEST(oa_icon_aggx) oa_icon_valid WHERE COALESCE(oa_icon_valid) != ''),

CASE
  WHEN (type::VARCHAR = ANY (ARRAY['motorway', 'primary', 'trunk', 'secondary', 'tertiary', 'service', 'residential', 'living_street', 'unclassified', 'pedestrian']))
  OR (type::VARCHAR = 'track' AND (tracktype::VARCHAR = ANY (ARRAY['grade1','grade2']))) THEN true
  ELSE false
END AS wide,

CASE
  WHEN
    network_aggx && ARRAY['iwn','nwn','rwn'] THEN true
  ELSE false
END AS long,

CASE
  WHEN oa_country IS NOT NULL AND oa_country::VARCHAR != 'GB' THEN true
  ELSE false
END AS coloured

FROM join_6;

/*
-- drop aggregate columns from created table
ALTER TABLE swisstopo_cycling_flat_l_v2
DROP COLUMN IF EXISTS swisstopo_id_aggx,
DROP COLUMN IF EXISTS name_x_aggx,
DROP COLUMN IF EXISTS int_name_aggx,
DROP COLUMN IF EXISTS ref_aggx,
DROP COLUMN IF EXISTS network_aggx,
DROP COLUMN IF EXISTS operator_aggx,
DROP COLUMN IF EXISTS jel_aggx,
DROP COLUMN IF EXISTS colour_aggx,
DROP COLUMN IF EXISTS osmc_symbol_aggx,
DROP COLUMN IF EXISTS wiki_symbol_aggx,
DROP COLUMN IF EXISTS wikidata_aggx,
DROP COLUMN IF EXISTS wikipedia_aggx,
DROP COLUMN IF EXISTS dms_id_aggx,
DROP COLUMN IF EXISTS oa_colour_x_aggx,
DROP COLUMN IF EXISTS oa_icon_aggx;
*/

-- create id primary column in new table
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY;

-- create collapse column of unique ordered values of array column oa_colour_aggx
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_colour_aggx_coll VARCHAR[];

-- do collapse update for oa_colour_aggx_coll using returning descending order
UPDATE swisstopo_cycling_flat_l_v2
SET oa_colour_aggx_coll = (
  SELECT ARRAY_AGG(x) FROM
  (SELECT DISTINCT(x) FROM
  (SELECT unnest(
  ARRAY_REMOVE(ARRAY_REMOVE(oa_colour_aggx , NULL) , '')
  ) AS x ORDER BY x DESC) xoo) koo
);

-- add integer columns to receive index positions of their values in oa_colour_aggx_coll
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_colour_red INTEGER;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_colour_blue INTEGER;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_colour_green INTEGER;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_colour_yellow INTEGER;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_colour_purple INTEGER;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_colour_rest INTEGER;

-- create collapse column of unique ordered values of array column oa_icon_aggx
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_icon_aggx_coll VARCHAR[];

-- do collapse update for oa_icon_aggx (order not important here, but used anyways)
UPDATE swisstopo_cycling_flat_l_v2
SET oa_icon_aggx_coll = (
  SELECT ARRAY_AGG(x) FROM
  (SELECT DISTINCT(x) FROM
  (SELECT unnest(
  ARRAY_REMOVE(ARRAY_REMOVE(oa_icon_aggx , NULL) , '')
) AS x ORDER BY x ASC) xoo) koo
);
-- add varchar columns to receive indexed values in oa_icon_aggx_coll
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_icon_1 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_icon_2 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_icon_3 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_icon_4 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS oa_icon_5 VARCHAR;

-- do updates for columns to receive index positions of their values in oa_colour_aggx_coll
UPDATE swisstopo_cycling_flat_l_v2
SET oa_colour_red = COALESCE(ARRAY_POSITION(oa_colour_aggx_coll, 'A'), 0),
oa_colour_blue = COALESCE(ARRAY_POSITION(oa_colour_aggx_coll, 'B'), 0),
oa_colour_green = COALESCE(ARRAY_POSITION(oa_colour_aggx_coll, 'C'), 0),
oa_colour_yellow = COALESCE(ARRAY_POSITION(oa_colour_aggx_coll, 'D'), 0),
oa_colour_purple = COALESCE(ARRAY_POSITION(oa_colour_aggx_coll, 'E'), 0),
oa_colour_rest = COALESCE(ARRAY_POSITION(oa_colour_aggx_coll, 'F'), 0);

-- do updates for columns to receive indexed values in oa_icon_aggx_coll
UPDATE swisstopo_cycling_flat_l_v2
SET oa_icon_1 = oa_icon_aggx_coll[1],
oa_icon_2 = oa_icon_aggx_coll[2],
oa_icon_3 = oa_icon_aggx_coll[3],
oa_icon_4 = oa_icon_aggx_coll[4],
oa_icon_5 = oa_icon_aggx_coll[5];

-- ##########################################################################################################

-- ref label
-- create collapse column of values of array column ref_aggx
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_aggx_coll VARCHAR[];

-- do collapse update for ref_aggx (order not important here, but used anyways)
UPDATE swisstopo_cycling_flat_l_v2
SET ref_aggx_coll = (
  SELECT ARRAY_AGG(x) FROM
  (SELECT DISTINCT(x) FROM
  (SELECT unnest(
  ARRAY_REMOVE(ARRAY_REMOVE(ref_aggx , NULL) , '')
) AS x ORDER BY x ASC) xoo) koo
);

-- add varchar columns to receive indexed values in ref_aggx_coll
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_1 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_2 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_3 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_4 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_5 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_6 VARCHAR;

-- do updates for columns to receive indexed values in ref_aggx_coll
UPDATE swisstopo_cycling_flat_l_v2
SET ref_1 = ref_aggx_coll[1],
ref_2 = ref_aggx_coll[2],
ref_3 = ref_aggx_coll[3],
ref_4 = ref_aggx_coll[4],
ref_5 = ref_aggx_coll[5],
ref_6 = ref_aggx_coll[6];

-- create ref_lbl column
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_lbl VARCHAR;

-- update ref label with ref_aggx_coll using line separator '\n'
UPDATE swisstopo_cycling_flat_l_v2
SET ref_lbl = (
CASE
WHEN CARDINALITY(ref_aggx_coll) > 0 THEN (SELECT STRING_AGG(u, E'\n' ORDER BY u) FROM UNNEST(ref_aggx_coll) u WHERE u IS NOT NULL)
ELSE ref_lbl
END);

-- ##########################################################################################################

-- ref name label

-- create ref_name_letter* columns
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_a VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_b VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_c VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_d VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_e VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_f VARCHAR;

-- update ref_name_letter* columns concatenating ref_letter* and name_letter* using 3 spaces --> '   '
UPDATE swisstopo_cycling_flat_l_v2
SET ref_name_a = (
  CASE
  WHEN COALESCE(name_a, '') != '' AND COALESCE(ref_a, '') != '' THEN ref_a||'   '||name_a
  WHEN COALESCE(name_a, '') != '' AND COALESCE(ref_a, '') = '' THEN name_a
  WHEN COALESCE(name_a, '') = '' AND COALESCE(ref_a, '') != '' THEN ref_a
  ELSE ref_name_a
  END
),
ref_name_b = (
  CASE
  WHEN COALESCE(name_b, '') != '' AND COALESCE(ref_b, '') != '' THEN ref_b||'   '||name_b
  WHEN COALESCE(name_b, '') != '' AND COALESCE(ref_b, '') = '' THEN name_b
  WHEN COALESCE(name_b, '') = '' AND COALESCE(ref_b, '') != '' THEN ref_b
  ELSE ref_name_b
  END
),
ref_name_c = (
  CASE
  WHEN COALESCE(name_c, '') != '' AND COALESCE(ref_c, '') != '' THEN ref_c||'   '||name_c
  WHEN COALESCE(name_c, '') != '' AND COALESCE(ref_c, '') = '' THEN name_c
  WHEN COALESCE(name_c, '') = '' AND COALESCE(ref_c, '') != '' THEN ref_c
  ELSE ref_name_c
  END
),
ref_name_d = (
  CASE
  WHEN COALESCE(name_d, '') != '' AND COALESCE(ref_d, '') != '' THEN ref_d||'   '||name_d
  WHEN COALESCE(name_d, '') != '' AND COALESCE(ref_d, '') = '' THEN name_d
  WHEN COALESCE(name_d, '') = '' AND COALESCE(ref_d, '') != '' THEN ref_d
  ELSE ref_name_d
  END
),
ref_name_e = (
  CASE
  WHEN COALESCE(name_e, '') != '' AND COALESCE(ref_e, '') != '' THEN ref_e||'   '||name_e
  WHEN COALESCE(name_e, '') != '' AND COALESCE(ref_e, '') = '' THEN name_e
  WHEN COALESCE(name_e, '') = '' AND COALESCE(ref_e, '') != '' THEN ref_e
  ELSE ref_name_e
  END
),
ref_name_f = (
  CASE
  WHEN COALESCE(name_f, '') != '' AND COALESCE(ref_f, '') != '' THEN ref_f||'   '||name_f
  WHEN COALESCE(name_f, '') != '' AND COALESCE(ref_f, '') = '' THEN name_f
  WHEN COALESCE(name_f, '') = '' AND COALESCE(ref_f, '') != '' THEN ref_f
  ELSE ref_name_f
  END
);

-- create ref_name_aggx column
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_aggx VARCHAR[];

-- update using array of ref_name_letter* columns
UPDATE swisstopo_cycling_flat_l_v2
SET ref_name_aggx = ARRAY[ref_name_a, ref_name_b, ref_name_c, ref_name_d, ref_name_e, ref_name_f]::VARCHAR[];

-- create collapse column of values of array column ref_name_aggx
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_aggx_coll VARCHAR[];

-- do collapse update for ref_name_aggx (order not important here, but used anyways)
UPDATE swisstopo_cycling_flat_l_v2
SET ref_name_aggx_coll = (
  SELECT ARRAY_AGG(x) FROM
  (SELECT DISTINCT(x) FROM
  (SELECT unnest(
  ARRAY_REMOVE(ARRAY_REMOVE(ref_name_aggx , NULL) , '')
) AS x ORDER BY x ASC) xoo) koo
);

-- add varchar columns to receive indexed values in ref_name_aggx_coll
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_1 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_2 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_3 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_4 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_5 VARCHAR;
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_6 VARCHAR;

-- do updates for columns to receive indexed values in ref_name_aggx_coll
UPDATE swisstopo_cycling_flat_l_v2
SET ref_name_1 = ref_name_aggx_coll[1],
ref_name_2 = ref_name_aggx_coll[2],
ref_name_3 = ref_name_aggx_coll[3],
ref_name_4 = ref_name_aggx_coll[4],
ref_name_5 = ref_name_aggx_coll[5],
ref_name_6 = ref_name_aggx_coll[6];

-- create ref_name_lbl column
ALTER TABLE swisstopo_cycling_flat_l_v2 ADD COLUMN IF NOT EXISTS ref_name_lbl VARCHAR;

UPDATE swisstopo_cycling_flat_l_v2
SET ref_name_lbl = (
CASE
WHEN CARDINALITY(ref_name_aggx_coll) > 0 THEN (SELECT STRING_AGG(u, E'\n' ORDER BY u) FROM UNNEST(ref_name_aggx_coll) u WHERE u IS NOT NULL)
ELSE ref_name_lbl
END);

-- ##########################################################################################################
/*
-- create indexes in new table
DROP INDEX IF EXISTS swisstopo_cycling_flat_l_v2_geohash_idx CASCADE;
CREATE INDEX swisstopo_cycling_flat_l_v2_geohash_idx ON swisstopo_cycling_flat_l_v2 USING btree (st_geohash(st_transform(st_setsrid((box2d(geometry))::geometry, 3857), 4326))) WITH (FILLFACTOR = 100);
CLUSTER VERBOSE swisstopo_cycling_flat_l_v2_geohash_idx ON swisstopo_cycling_flat_l_v2;

DROP INDEX IF EXISTS swisstopo_cycling_flat_l_v2_idx CASCADE;
CREATE INDEX swisstopo_cycling_flat_l_v2_idx ON swisstopo_cycling_flat_l_v2 USING gist (geometry);
*/

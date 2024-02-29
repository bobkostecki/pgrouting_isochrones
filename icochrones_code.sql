--Listing 1.1 Uploading extensions to databse
CREATE EXTENSION pgrouting CASCADE;
--Import road data to database using PostGIS Shapefile Import/Export Manager 
--Listing 1.2 transforming spatial reference of imported data to local geodetic system

ALTER TABLE osm_roads
ALTER COLUMN geom 
TYPE Geometry(linestring, 2180) 
USING ST_Transform(geom, 2180);

--Listing 1.3 creating conatiner schema gis for network data
DROP SCHEMA gis CASCADE;
CREATE SCHEMA gis;

--Listing 1.4 Creating table with polygon of network range
CREATE TABLE gis.networkrange AS 
SELECT 'range' AS name ,st_union(geom)::geometry(polygon,2180) AS geom 
FROM powiaty WHERE jpt_nazwa_ IN ('powiat Poznań', 'powiat poznański')

--Listing 1.5 Creating table with filtered roads regarding road type and range of network
CREATE TABLE gis.roads AS 
WITH g AS 
(SELECT st_union(geom) AS geom FROM gis.networkrange)
SELECT r.* AS geom FROM osm_roads r join g 
ON st_contains(g.geom, r.geom) where maxspeed <= 90 
and fclass not in 
('bridleway','footway', 'steps', 'path', 'busway', 
 'service','pedestrian', 'motorway', 'motorway_link', 'trunk', 'trunk_link');
alter table gis.roads add primary key (gid);

--Listing 1.5.1 handling multi-level junctions 
alter table gis.roads
add column bridge_tunnel boolean;
update gis.roads
set bridge_tunnel = (
SELECT
CASE
 WHEN bridge='T' THEN true
 WHEN tunnel='T' THEN true
ELSE
 false
END);

--Listing 1.5.2 handling bike speed
alter table gis.roads
add column bike_speed integer;
update gis.roads
set bike_speed = (
SELECT
CASE
 WHEN fclass in ('unclassified','track','track_grade1', 'track_grade2', 'track_grade3','track_grade4', 'track_grade5') THEN 10
ELSE
 20
END);


--Listing 1.6 table creating with edges up to 20m
CREATE TABLE gis.roads_20 AS
SELECT row_number() OVER (ORDER BY gid asc)AS gid,gid AS old_id, bike_speed, bridge_tunnel, ST_LineSubstring(geom, 20.00*n/length,
  CASE
	WHEN 20.00*(n+1) < length THEN 20.00*(n+1)/length
	ELSE 1
  END) ::geometry(linestring,2180) As geom 
FROM
  (SELECT roads.gid,
  bike_speed ,
  bridge_tunnel,
  ST_LineMerge(roads.geom) AS geom, --st_linemerge in the case connected multilines
  ST_Length(roads.geom) As length
  FROM gis.roads
  ) AS t
CROSS JOIN generate_series(0,10000) AS n
WHERE n*20.00/length < 1;

--Listing 1.7 Query performing correction of the net topology 
SELECT pgr_nodeNetwork('gis.roads', 0.01, 'gid', 'geom',rows_where:= 'bridge_tunnel= false', outall:=true );
SELECT pgr_nodeNetwork('gis.roads_20', 0.01, 'gid', 'geom',rows_where:= 'bridge_tunnel= false', outall:=true );



--Listing 1.8 Query adding cost columns to netowrk table
ALTER TABLE gis.roads_20_noded ADD COLUMN cost_len double precision;
UPDATE gis.roads_20_noded SET cost_len = ST_Length(geom);

ALTER TABLE gis.roads_20_noded ADD COLUMN cost_time double precision;
ALTER TABLE gis.roads_20_noded ADD COLUMN bike_speed smallint;
update gis.roads_20_noded p
set bike_speed = r.bike_speed from gis.roads_20 r where p.old_id =gid;
update gis.roads_20_noded 
set cost_time =cost_len/1000/bike_speed*60;

--Listing 1.9 Query responsible for topology creation
SELECT pgr_createTopology('gis.roads_noded', 0.01,'geom','id');
SELECT pgr_createTopology('gis.roads_20_noded', 0.01,'geom','id');


--Listing 1.10 Query peforming topology analysis
SELECT pgr_analyzeGraph('gis.roads_20_noded', 0.01,'geom','id');

--Listing 1.11 Queries adding optional parameters with values
ALTER TABLE gis.roads_20_noded
ADD COLUMN x1 double precision,
ADD COLUMN y1 double precision,
ADD COLUMN x2 double precision,
ADD COLUMN y2 double precision;

UPDATE gis.roads_20_noded
SET x1 = st_x(st_startpoint(geom)),
    y1 = st_y(st_startpoint(geom)),
    x2 = st_x(st_endpoint(geom)),
    y2 = st_y(st_endpoint(geom));
	

--Listing 2.1 Queries testing algorithms pgr_dijkstra and pgr_aStar
SELECT * FROM pgr_dijkstra(
'SELECT id, source::INTEGER, target::bigint,
cost_len::double precision AS cost
FROM gis.roads_20_noded',1852,2135,false)

SELECT * FROM pgr_aStar(
'SELECT id, source::INTEGER, target::bigint,
cost_len::double precision AS cost,x1,y1,x2,y2
FROM gis.roads_20_noded',1852,2135,false
);

--Listing 2.2 testing pgr_dijkstra with geometry
SELECT seq, node, edge,cost,agg_cost,geom 
FROM pgr_dijkstra('SELECT id,
source::integer, target::integer,
cost_len::double precision AS cost
FROM gis.roads_20_noded',1852,2135, false)AS di
join gis.roads_20_noded pt ON di.edge=pt.id;

--Listing 2.7 Code of alphAShape wrapper function with srid transformation
CREATE OR REPLACE FUNCTION gis.alphAShape(x double precision, y double precision, dim integer)
returns table (geom geometry) AS
$$
WITH dd AS (
SELECT *
FROM pgr_drivingDistance(
'SELECT id, source, target, length::double precision AS cost
FROM gis.roads_20_noded',
(SELECT id::integer FROM gis.roads_50_noded_vertices_pgr
ORDER BY the_geom <-> ST_Transform(ST_GeometryFromText('POINT('||x||' '||y||')',4326),2180) LIMIT 1),
dim, false)
)
SELECT ST_ConcaveHull(st_collect(the_geom),0.1) AS geom
FROM gis.roads_20_noded_vertices_pgr net
INNER JOIN dd ON net.id=dd.node;
$$
LANGUAGE 'sql';

--Listing 2.8  Query testing alphAShape function with exmaple parameters
SELECT * FROM gis.alphAShape(16.94,52.36,3000);

--Listing 2.9 Query testing alphAShape function with multiple distances
SELECT n, gis.alphAShape( 16.9,52.4,n)geom
FROM 
generate_series(1000,6000,1000)n ORDER BY n desc ;


--Listing 2.10 wrapper function for alphashape with cost_time and without srid transformation
CREATE OR REPLACE FUNCTION gis.alphAShapetime
(geom_point geometry, dim integer)
returns table (geom geometry) AS
$$
WITH dd AS (
SELECT *
FROM pgr_drivingDistance(
'SELECT id, source, target,cost_time::double precision AS cost
FROM gis.roads_20_noded',
(SELECT id::integer FROM gis.roads_20_noded_vertices_pgr
ORDER BY the_geom <-> geom_point LIMIT 1),
dim, false)
)
SELECT  ST_ConcaveHull(st_collect(the_geom),0.3) AS geom
FROM gis.roads_20_noded_vertices_pgr net
INNER JOIN dd ON net.id=dd.node;
$$
LANGUAGE 'sql';

--Listing 3.1 creating view with largest forests
create materialized view gis.ptzl_forest_500 as
select id, st_area(geom),geom from ptzl where rodzaj='Las' and st_area(geom)>500000;

--listing 3.2 simplyfing geometry 
create materialized view gis.ptzl_forest_500_union as
select (ST_dump(st_union(geom))).path[1] as id, (ST_dump(st_union(geom))).geom from gis.ptzl_forest_500;

--intersections with forest boundary
create materialized view gis.ptzl_forest_500_boundry as
select id, st_boundary(geom)geom from gis.ptzl_forest_500_union;

create materialized view gis.ptlz_forest_500_bundary_inter as
select row_number() over () as id, st_intersection(b.geom, r.geom)geom 
from gis.ptzl_forest_500_boundry b join gis.roads r on st_intersects(b.geom, r.geom);

--Listing 3.3 creating hexgrid subpolygons
create materialized view gis.hexgrid_1000 as
SELECT row_number() over () as id,ST_Intersection(grid.geom,g.geom) as geom
FROM
gis.networkrange AS g
INNER JOIN
ST_HexagonGrid(1000,g.geom) AS grid
ON st_intersects(g.geom, grid.geom);

create materialized view gis.hexgrid_2000 as
SELECT row_number() over () as id,ST_Intersection(grid.geom,g.geom) as geom
FROM
gis.networkrange AS g
INNER JOIN
ST_HexagonGrid(2000,g.geom) AS grid
ON st_intersects(g.geom, grid.geom);

--Listing 3.4 creaing views with polygons divided by hexgrid polygons
create materialized view gis.ptzl_forest_hex_1000 as
select id, st_multi(geom)geom from (
select row_number() over () as id, st_intersection(h.geom, p.geom) geom
from gis.ptzl_forest_500_union p join gis.hexgrid_1000 h on st_intersects(h.geom,p.geom)
)f where st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon');

create materialized view gis.ptzl_forest_hex_2000 as
select id, st_multi(geom)geom from (
select row_number() over () as id, st_intersection(h.geom, p.geom) geom
from gis.ptzl_forest_500_union p join gis.hexgrid_2000 h on st_intersects(h.geom,p.geom)
)f where st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon');

--Listing 3.5 creating centroids
create materialized view gis.ptzl_centroid_forest_1000 as
select id,  st_centroid(geom)geom from gis.ptzl_forest_hex_1000;

create materialized view gis.ptzl_centroid_forest_2000 as
select id,  st_centroid(geom)geom from gis.ptzl_forest_hex_2000;


--Listing 3.6 generating multiple isochrones for centroid points
create materialized view gis.isochrones_1000 as
with loc as (
select id, geom from gis.ptzl_centroid_forest_1000),
pol as (select t, gis.alphaShapetime(loc.geom,t)geom
from loc, 
generate_series(5,20,5)t ORDER BY t desc)
select t, st_union(geom)geom 
from pol group by t order by t desc;
--Query returned successfully in 25 min 43 secs.

create materialized view gis.isochrones_2000 as
with loc as (
select id, geom from gis.ptzl_centroid_forest_2000),
pol as (select t, gis.alphaShapetime(loc.geom,t)geom
from loc, 
generate_series(5,20,5)t ORDER BY t desc)
select t, st_union(geom)geom 
from pol group by t order by t desc;
--Query returned successfully in 11 min 27 secs.



--Listing 3.7 summary calculations
--summary for studied area
create materialized view gis.summary_1000 as
select t, 
sum(case when rodzaj='Wld' then
st_area(st_intersection(i.geom,p.geom))/1000000 end)::numeric(6,3) wld,
sum(case when rodzaj='Jrd' then
st_area(st_intersection(i.geom,p.geom))/1000000 end)::numeric(6,3) jrd
from gis.isochrones_1000 i join ptzb p on st_intersects(i.geom,p.geom)
group by t;

create materialized view gis.summary_2000 as
select t, 
sum(case when rodzaj='Wld' then
st_area(st_intersection(i.geom,p.geom))/1000000 end)::numeric(6,3) wld,
sum(case when rodzaj='Jrd' then
st_area(st_intersection(i.geom,p.geom))/1000000 end)::numeric(6,3) jrd
from gis.isochrones_2000 i join ptzb p on st_intersects(i.geom,p.geom)
group by t;

--summmary for individual communes
create materialized view gis.ptzb_gminy as
select jpt_nazwa_, st_union(st_intersection(g.geom,p.geom))geom
from ptzb p join gis.gminy_pp g on st_intersects(g.geom,p.geom) 
group by jpt_nazwa_;

create materialized view gis.summary_ptzbgminy1000 as
select jpt_nazwa_,
sum(case when t = 5 then
st_area(st_intersection(i.geom,p.geom))/st_area(p.geom)*100 end)::numeric(6,3) "5 min",
sum(case when t = 10 then
st_area(st_intersection(i.geom,p.geom))/st_area(p.geom)*100 end)::numeric(6,3) "10 min",
sum(case when t = 15 then
st_area(st_intersection(i.geom,p.geom))/st_area(p.geom)*100 end)::numeric(6,3) "15 min",
sum(case when t = 20 then
st_area(st_intersection(i.geom,p.geom))/st_area(p.geom)*100 end)::numeric(6,3) "20 min"
from gis.isochrones_1000 i join gis.ptzb_gminy p on st_intersects(i.geom,p.geom)
group by jpt_nazwa_;

select fclass, (sum(st_length(geom))/1000)::numeric(8,2)as len from gis.roads group by fclass order by len desc;
select bike_speed, (sum(st_length(geom))/1000)::numeric(8,2)as len from gis.roads group by bike_speed order by len desc;



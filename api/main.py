# -*- coding: utf-8 -*-
import duckdb
import functions_framework
import json
from datetime import datetime
import os

duckdb.sql(f"INSTALL httpfs;")
duckdb.sql(f"LOAD httpfs;")

duckdb.sql(f"INSTALL spatial;")
duckdb.sql(f"LOAD spatial;")

s3_access_key_id = os.getenv("API_GCS_ACCESS_KEY_ID")
s3_secret_access_key = os.getenv("API_GCS_ACCESS_KEY")
assert s3_access_key_id and s3_secret_access_key
duckdb.sql("SET s3_endpoint = 'storage.googleapis.com';")
duckdb.sql(f"SET s3_access_key_id = '{s3_access_key_id}';")
duckdb.sql(f"SET s3_secret_access_key = '{s3_secret_access_key}';")

# Build index on parquet file
duckdb.sql(
    f"CREATE TABLE IF NOT EXISTS solareignung AS SELECT * FROM read_parquet('s3://folimar-geotest-store001/landing/api_testdata/solareignung.parquet');"
)
duckdb.sql("CREATE INDEX IF NOT EXISTS idx_egid ON solareignung (egid);")


@functions_framework.http
def egid_lookup(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    start = datetime.now()
    if request_args and request_args.get("egid"):
        # BAD BAD BAD NO SANITIZATION
        egid = request_args["egid"].replace(";", "")
    else:
        egid = duckdb.sql(f"SELECT egid FROM solareignung USING SAMPLE 1;").fetchall()[
            0
        ][0]
    results = duckdb.sql(
        f"SELECT egid, area_ratio_original, flaeche, stromertrag, ST_AsGeoJSON(ST_FlipCoordinates(ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:2056', 'EPSG:4326'))) AS geom FROM solareignung WHERE egid={egid};"
    ).fetchall()

    response_dict = {"data": []}
    for r in results:
        response_dict["data"].append(
            {
                "area_ratio_original": float(r[1]),
                "flaeche": float(r[2]),
                "stromertrag": float(r[3]),
                "geometry": json.loads(r[4]),
            }
        )
    response_dict["egid"] = egid
    response_dict["total_flaeche"] = sum([r["flaeche"] for r in response_dict["data"]])
    response_dict["total_stromertrag"] = sum(
        [r["stromertrag"] for r in response_dict["data"]]
    )
    response_dict["processing_time_s"] = (datetime.now() - start).total_seconds()

    headers = {"Access-Control-Allow-Origin": "*"}
    return json.dumps(response_dict), 200, headers

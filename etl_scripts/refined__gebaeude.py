# -*- coding: utf-8 -*-
"""

"""

from util import PandaPath
import geopandas as gpd
import subprocess
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SOURCE_FILE_AV_GEBAEUDEFLAECHE = PandaPath(
    "/workspaces/pretty_panda/data/landing/ch.swisstopo-vd.amtliche-vermessung/gebaeudeflaeche.fgb"
)
SOURCE_FILE_AV_GEBAEUDENUMMER = PandaPath(
    "/workspaces/pretty_panda/data/landing/ch.swisstopo-vd.amtliche-vermessung/gebaeudenummer.fgb"
)
SOURCE_FILE_AV_GWR = PandaPath(
    "/workspaces/pretty_panda/data/landing/ch.bfs.gwr/buildings.fgb"
)

SCRATCH_FOLDER = PandaPath("/workspaces/pretty_panda/data/scratch/gebaeude")

SINK_FILE = PandaPath("/workspaces/pretty_panda/data/refined/gebaeude/gebaeude.fgb")


def process():
    logging.info(f"Reading data from {SOURCE_FILE_AV_GEBAEUDEFLAECHE}...")
    gdf_av_flaeche = gpd.read_file(
        SOURCE_FILE_AV_GEBAEUDEFLAECHE.as_gdal(), engine="pyogrio"
    )
    assert gdf_av_flaeche.crs.to_epsg() == 2056
    assert gdf_av_flaeche.geometry.is_valid.all()
    cols_to_rename_and_keep = {
        "R1_GUELTI1": "gueltigkeit",
        "R1_GUELTI2": "gueltigkeitsdatum",
        "QUALITAET_": "qualitaetsstandard",
        "R1_NBIDENT": "nbident",
        "R1_IDENTIF": "mutationsnummer",
        "geometry": "geometry",
    }
    gdf_av_flaeche = gdf_av_flaeche.rename(columns=cols_to_rename_and_keep)[
        cols_to_rename_and_keep.values()
    ]
    gdf_av_flaeche["flaeche_m2"] = gdf_av_flaeche.area.astype("int")

    logging.info(f"Reading data from {SOURCE_FILE_AV_GEBAEUDENUMMER}...")
    gdf_av_nummer = gpd.read_file(
        SOURCE_FILE_AV_GEBAEUDENUMMER.as_gdal(), engine="pyogrio"
    )
    assert gdf_av_nummer.crs.to_epsg() == 2056
    assert gdf_av_nummer.geometry.is_valid.all()
    cols_to_rename_and_keep = {
        "R1_NUMMER": "assekuranznummer",
        "R1_GWR_EGI": "egid",
        "geometry": "geometry",
    }
    gdf_av_nummer = gdf_av_nummer.rename(columns=cols_to_rename_and_keep)[
        cols_to_rename_and_keep.values()
    ]

    logging.info(f"Joining AV gebaeudenummer to AV gebaeudeflaeche...")
    gdf_av_flaeche.sindex
    gdf_av_nummer.sindex
    gdf_av_flaeche = gpd.sjoin_nearest(
        gdf_av_flaeche, gdf_av_nummer, how="left", max_distance=10, rsuffix="_av_nummer"
    )
    del gdf_av_nummer

    logging.info(f"Reading data from {SOURCE_FILE_AV_GWR}...")
    gdf_av_gwr = gpd.read_file(SOURCE_FILE_AV_GWR.as_gdal(), engine="pyogrio")
    assert gdf_av_gwr.crs.to_epsg() == 2056
    assert gdf_av_gwr.geometry.is_valid.all()

    logging.info(f"Joining AV gwr to AV gebaeudeflaeche...")
    gdf_av_gwr.sindex
    gdf_av_flaeche = gpd.sjoin_nearest(
        gdf_av_flaeche, gdf_av_gwr, how="left", max_distance=10, rsuffix="_gwr"
    )
    del gdf_av_gwr

    logging.info(f"Prepare for export")
    gdf_av_flaeche = gdf_av_flaeche[
        gdf_av_flaeche.geom_type.isin(["Polygon", "MultiPolygon"])
    ]

    scratch_file = SCRATCH_FOLDER / "gebaeude.fgb"
    logging.info(f"Writing temprary file to {scratch_file}...")
    SCRATCH_FOLDER.mkdir(parents=True, exist_ok=True)
    gdf_av_flaeche.to_file(scratch_file, engine="pyogrio")

    return scratch_file


def upload(scratch_file: PandaPath):
    logging.info(f"Uploading to {SINK_FILE}...")
    command = [
        "ogr2ogr",
        "-progress",
        "--debug",
        "OFF",
        "-makevalid",
        "-nlt",
        "PROMOTE_TO_MULTI",
        SINK_FILE.as_gdal(),
        scratch_file.as_gdal(),
    ]
    subprocess.run(command, check=True)


def refined__solarpotential():
    logging.info(f"Start {__name__}")
    scratch_file = process()
    upload(scratch_file)
    logging.info(f"All done!")


if __name__ == "__main__":
    refined__solarpotential()

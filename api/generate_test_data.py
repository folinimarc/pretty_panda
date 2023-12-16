# -*- coding: utf-8 -*-
import subprocess
from upath import UPath

sink_path = UPath(
    "gs://folimar-geotest-store001/landing/api_testdata/solareignung.parquet"
)
source_path = UPath("/workspaces/pretty_panda/data/poc_data/demo_biel.fgb")

sink_path.parent.mkdir(parents=True, exist_ok=True)
sink_path.unlink(missing_ok=True)
cmd = [
    "ogr2ogr",
    "-progress",
    f"{str(sink_path).replace('gs://', '/vsigs/')}",
    f"{source_path}",
]
subprocess.run(cmd, check=True)

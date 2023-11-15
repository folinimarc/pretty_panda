# -*- coding: utf-8 -*-
import etl_scripts.utils_gcs as u
import requests

meta_url = "https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/meta.txt"

for i, line in enumerate(requests.get(meta_url).text.splitlines(), 1):
    zip_url, timestamp = line.split(" ")
    print(i, ") ", timestamp, " --> ", zip_url)
    requests.get(zip_url).content


# # Prepare tmp dir
# tmp_dir = Path("./tmp")
# shutil.rmtree(tmp_dir, ignore_errors=True)
# os.makedirs(tmp_dir, exist_ok=True)

# # Download and extract
# r = requests.get(zip_file_url)
# z = zipfile.ZipFile(io.BytesIO(r.content))
# z.extractall(tmp_dir)

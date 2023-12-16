gcloud functions deploy egid_lookup \
    --project=folimar-geotest \
    --gen2 \
    --runtime=python311 \
    --region=europe-west9 \
    --source=. \
    --entry-point=egid_lookup \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=sa-gcs-adm-geotest-store001@folimar-geotest.iam.gserviceaccount.com \
    --memory=16GiB \
    --cpu=4

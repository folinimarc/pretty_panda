# Project "Pretty Panda"

## Goal
This repository is a playground to experiment on a collection of loosely related components, to
1. enable developers to perform geospatial processing locally through a containerized environment.
2. demonstrate how to interact with large amounts of static geospatial data in low cost blob storage.
3. provide a framework to enable periodic batch processing of geospatial workflows.

## Case study
Open data from Switzerland will be used, because the data is high quality, readiliy available and interesting.

## Design guidelines
- Optimize for cost efficiency for usecases where small to medium sized static, read-only datasets are produced through geospatial workflows through a low frequency batch process.
- Leverage Google Cloud infrastructure where possible.
- Allow multiple developers to efficiently perform exploratory analysis on stored datasets.

## Milestones
| Milestone                                                                                         | Status      |
| ------------------------------------------------------------------------------------------------- | ----------- |
| Containerized python geo-ecosystem available on github container registry.                        | Done        |
| Find way to interact with blob storage and filesystem in a unified way.                           | Done        |
| Create processing scripts to get building roof solar potential for city of Biel.                  | Started     |
| Test deployment of script in containerized processing environment on Google Batch.                | Not started |
| Create ETL pipelines to batch-process building root solar potential for Switzerland periodically. | Not started |
| Test deployment of ETL pipeline in containerized processing environment on Google Batch.          | Not started |
|                                                                                                   |             |

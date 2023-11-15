# Project "Pretty Panda"

## Goal
This repository is a playground to develop a collection of loosely coupled components
which...
1. enable developers to efficiently perform geospatial processing locally through a containerized environment.
2. demonstrate how to interact with large amounts of static geospatial data in low cost blob storage.
3. provide a framework to enable periodic batch processing of geospatial workflows.

## Case study
Open data from Switzerland will be used for demonstration purposes.

## Design guidelines
- Optimize for cost efficiency for usecases where small to medium sized static, read-only datasets are produced through geospatial workflows through a low frequency batch process.
- Leverage Google Cloud infrastructure where possible.
- Allow multiple developers to efficiently perform exploratory analysis on stored datasets.

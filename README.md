# Project "Pretty Panda"

## Goal
This repository is a collection of independent loosely coupled components to enable developers to efficiently prototype
geospatial processing locally. Once the processing steps to create a new dataset work
in the local environment, it should be straight-forward to bring them in a
production environment for periodic batch execution.

## Design requirements
- Allow flexible and rapid local prototyping involving spatial processing.
- Store third party datasets and processing results in an organized manner.
- Allow for periodic batch re-processing of the data.
- Leverage Google Cloud infrastructure if possible.
- Low cost as a priority requirement.

## Components
### Data store
We leverage Cloud Storage as cost-efficient storage for geospatial data. We leverage cloud-native data formats where possible.

### Processing environment
Various processing capabilities are bundled using container technology. This enables consistency across development and production workflows.
Priotity is given to the Python Geo-Ecosystem, followed by pyQGIS and R capabilities.

### Local development
We leverage Jupyterlab as a UI for development due to its integrated visualization capabilities and extensive plugin system.

### Batch processing
We leverage Google Batch to provide the infrastructure for batch runs.

### Orchestration
Use Cloud Scheduler to trigger the batch jobs.

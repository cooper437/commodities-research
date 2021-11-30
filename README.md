## Fund Research

#### Overview

A place for R&D of market data and automation of trading strategies.

#### Repository Infomation
For now we will keep everything in a mono-repo for convenience. Each folder at the root represents a service or area with single theme/responsibility:
+ notebooks - Jupyter notebooks for data discovery, analysis and experimentiation
+ data - raw and processed data for analysis/automation (this will likely be replaced by a more sophisticated approach for versioning data but will suffice for now)
+ etl - scripts for data extraction, transformation, and loading

There are child README.md files inside each subfolder that instruct on how to setup your dev environment for that particular part of the repository.

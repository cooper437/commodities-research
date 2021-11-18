## Notebooks R&D

#### Overview
Discovery, research, and analysis of various datasets.

#### Major Dependencies
The following top level dependecies are needed:
+ [Python 3](https://www.python.org/downloads/) - Use the latest 3.x release available (3.10 at the time this was written)
+ [Anaconda](https://docs.anaconda.com/anaconda/install/index.html) - A pacakage manager for notebooks and data science workflows.

#### Environment Setup

After installing python and anaconda the following instructions will setup a local dev environment:

1. Make sure you are in the notebooks directory of the repo
2. Install the python dependencies
```
conda env create -f environment.yml
```
3. Activate the conda environment
```
conda activate fund-research-analysis
```
4. Start up JupyterLab
```
jupyter lab
```
5. View and run your notebooks in the web browser

#### Resources
+ [Jupyter Lab Docs](https://jupyterlab.readthedocs.io/en/stable/index.html_)
+ [Conda Docs](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/index.html)
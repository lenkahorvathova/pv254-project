# PV254 Project

Project for [PV254 Recommender Systems](https://is.muni.cz/predmet/fi/podzim2017/PV254) on FI MUNI.

## Team: 
* Lenka Horváthová
* Lucie Kurečková
* Markéta Vítková

## Setup
* Clone this GIT repository: 
    ```
        $ git clone https://github.com/lenkahorvathova/pv254-project.git
    ```
* Set PYTHONPATH variable:
    ```
        $ cd pv254-project
        $ export PYTHONPATH=`pwd`
    ```
* Download review data and metadata files of **Toys & Games** from [Amazon Product Data Website](http://jmcauley.ucsd.edu/data/amazon/) into *data* folder:
    ```
        $ mkdir data
        ...
        $ ls data
            meta_Toys_and_Games.json.gz     reviews_Toys_and_Games_5.json.gz
    ```
* Create and activate Python environment:
    ```
        $ python3 -m venv venv
        $ source venv/bin/activate
    ```
    - to deactivate Python environment: `deactivate`
* Install requirements:
    ```
        $ pip install requirements.txt 
    ```
* Set up and populate a database (~ 6 min):
    ```
        $ python3 scripts/setup_db.py --review_file "data/reviews_Toys_and_Games_5.json.gz" --meta_file "data/meta_Toys_and_Games.json.gz"
    ```
* Check data in the database:
<<<<<<< HEAD
    - connect to DB: `$ sqlite3 data/amazon_product_data.db`
    - list tables in DB: `> .tables`
    - view a schema of a table: `> .schema <table>`
    - a query example: `> SELECT * FROM review LIMIT 10;`
    - exit sqlite3 program: `> .quit`
* Set FLASK_APP variable:
    ```
        $ export FLASK_APP=frontend/server.py
    ```
* Run the application locally and go to 'http://127.0.0.1:5000/':
    ```
        $ python3 frontent/server.py
    ```

## Directory and File Structure

```
    pv254-project/
    └─── data/
    |   |   amazon_product_data.db
    │   │   meta_Toys_and_Games.json.gz
    │   │   reviews_Toys_and_Games_5.json.gz
    |
    │   README.md
    │   schema.sql
    |
    └─── scripts/
    |   │   setup_db.py
    |   |   ...
    │   
    └─── venv/
        |   ...
```

## Citation 

> R. He, J. McAuley. Modeling the visual evolution of fashion trends with one-class collaborative filtering. WWW, 2016
> J. McAuley, C. Targett, J. Shi, A. van den Hengel. Image-based recommendations on styles and substitutes. SIGIR, 2015
rewrite project code from week 6-7:

- Aim is to restructure the previous ETL model into a more modular, objectoriented model, that can be run from a single main.py script and with a standardised interfacer so that the invidual components can be swapped

Overall approach:

Three main classes contained in three separate scripts:

Extractor class -> handles data extraction from CSV files, MySQL DB and API endpoints, returns dataframes (pandas)

Transformer class -> handles data transformations

Loader class -> Handles the loading of data into a MySQL database

To-do/potential improvements:

- Proper error logging rather than print statements (and improving/streamlining existing prints statements)

- Add more info to this documents re. proces and functions
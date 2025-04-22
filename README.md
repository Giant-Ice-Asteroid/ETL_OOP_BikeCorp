To run the entire ETL proces:

1. install dependencies listed in requirements.txt
2. Run setup_source_database.py to set up and load data into ProductDB
3. Run setup_target_database.py to set up BikeCorpDB
4. Run fastapi run run_api.py to start up API
5. In the same terminal run python main.py
6. If succesful, the data will have been extracted, transformed and loaded into BikeCorpDB! 

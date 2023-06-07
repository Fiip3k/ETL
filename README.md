# ETL

1. install requirements.txt (venv suggested)

2. config.py file is required within the script directory to connect to my hostings

3. Kafka:
- run consumerTest.py and leave it running
- run producerTest.py once
- check consumerTest.py console for received messages
- you can play with the message variable to see what happens

4. Azure Data Lake:
- run azureTest.py to upload the test.csv file to "newdata/test/" directory
- you can try to play with the files but don't make them too big

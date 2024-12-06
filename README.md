# DE course final including ETL pipeline

main.py - the main dag, needs to be placed in dag folder after installation for airflow sheduler to notice it and take it into work.

docker-compose.yml - file Docker-container creating and start which will orcestrate data using Airflow and use Clickhouse/postgress for stroing the data.

Dockerfile - mainly needed to include java for pyspark and frameworks installation. 

russian_houses.csv - this file can be downloaded using the first task download_file which is commented right now, but for simplicity 
it is added here to start working with it. It should be stored in dag folder as well for sheduler to notice it(or work with it through Xcom).

small.csv - a smaller version of previous file for tryout (may not work with the current pipeline). 


Instructions:

  • Download the files using git clone or manually. 

  • after downloading, use docker build -t airflow-with-java . command (dot included) to install the first image. If there is an issue during the process of airflow pipeline a solution might be to change Java version to 17 and try to adapt different versions of pyspark to it. 
  version downloads for docker can be found here -https://download.oracle.com/java/23/latest/jdk-23_linux-x64_bin.tar.gz

  • After the installation is done, in the project folder run this command - docker-compose up --build

  • The installation will take around 15 minutes. After that the project can be activated running docker-compose up


Results:
- The pipeline will perform 3 tasks - creating databases with a specific structure for the data in the files (create_table_in_clickhouse),
cleaning and analyzing the data and then saving the files in dag (read_clean_data). and finally pushing the data to the db. 
There is also an additional function/task for downloading the file for full automatization.

Currently noticed some issue with the second function when running the process due to issue with PC. Needs to get back to it. 
If there is an issue during the process of airflow pipeline a solution might be to change Java version to 17 and try to adapt different versions of pyspark to it. 
version downloads for docker can be found here -https://download.oracle.com/java/23/latest/jdk-23_linux-x64_bin.tar.gz

References:
russian_houses.csv link - https://disk.yandex.ru/d/pilqVLKqwdUOxg
small.csv link - https://disk.yandex.ru/d/5MjUOaf08kUQJA

project task - https://stepik.org/lesson/893204/step/3?unit=898147

# airflow-postgres


## Projects Description
1. API project- Extract data from SWAPI and process the data then write to postgres database
2. Webscraping project- Webscraping data from rottentomatoes website and process the data then write to postgres database
3. Flask project- Build an Flask application to access and modify the data in employees.csv

## _Instruction_

### Airflow- API and webscraping project
1. Open the parent directory of the project in VS code or your preferred code editors and open the **terminal**.

2. Execute **. start.sh** in the terminal to setup the docker environments.

3. Setup email alart
   i. Setup app passwords in https://security.google.com/settings/security/apppasswords
   ii. Setup smtp under **airflow.cfg** as below (The rests remind the same)
       smtp_host = smtp.gmail.com
       smtp_user = {your email}
       smtp_password = {password generated from app password}
       smtp_port = 587
       smtp_mail_from = {your email}

4. Open web browser and go to **http://localhost:8080/**
   user: airflow
   password: airflow

5. Setup connection to swapi
   i. Click on Admin on the top bar
   ii. Click on Connection and add a new connection name **swapi**
   iii. Change the conn Type to **HTTP**
   iv. Input Host as **https://swapi.dev/api/**
   v. Save it

6. Setup connection to postgres
   i. Click on Admin on the top bar
   ii. Click on Connection and add a new connection name **postgres**
   iii. Change the conn Type to **Postgres**
   iv. Input Host as **postgres**
   v. Input Schema as **airflow_db**
   vi. Input Login as **airflow**
   vii. Input Password as **airflow**
   viii. Input Port as **5432**
   ix. Save it

7. Setup postgres in localhost
   i. Install DBeaver
   ii. Create New Database Connection
   iii. Input Host as **locahost**
   iv. Input Database as **airflow_db**
   v. Input Username as **airflow**
   vi. Input Password as **airflow**
   vii. Input Port as **32769**
   viii. Save it

8. Switch on the DAG **1_api_task** and let it run or create on play button on the actions bar to trigger the DAG manually

9. Switch on the DAG **2_web_scraping_task** and let it run or create on play button on the actions bar to trigger the DAG manually

10. Close the project by executing **. stop.sh** in terminal


### Flask project
1. Executing **python src/3_flask_tasks.py** to initialize flask project

#### Get all employees details 
2. Open Postman and make sure you have selected the "GET" method

3. Enter the URL http://127.0.0.1:5000/employee in the request URL field. (192.168.1.101 for LAN connection)

4. Click the "Send" button to send the GET request.

#### Get particular employee details 
5. Open Postman and make sure you have selected the "GET" method

6. Enter the URL http://127.0.0.1:5000/employee/{employee_id} in the request URL field. (192.168.1.101 for LAN connection)

7. Click the "Send" button to send the GET request.


#### Add new employee
8. Open Postman and make sure you have selected the "POST" method

9. Enter the URL http://127.0.0.1:5000/employee in the request URL field. (192.168.1.101 for LAN connection)

10. Go to the "Body" tab.

11. Select the "raw" option.

12. Choose "JSON" from the dropdown menu next to the raw option.

13. In the request body, enter the JSON data for the employee you want to add
    {
        "id": 9,
        "employee": "Alan",
        "gender": "M",
        "age": 32,
        "salary": 3000,
        "town": "Woodlands"
    }

14. Click the "Send" button to send the POST request.

#### Delete employee
15. Open Postman and make sure you have selected the "DELETE" method

16. Enter the URL http://127.0.0.1:5000/employee/{employee_id} in the request URL field. (192.168.1.101 for LAN connection)

17. Click the "Send" button to send the DELETE request.

#### Update employee
18. Open Postman and make sure you have selected the "POST" method

19. Enter the URL http://127.0.0.1:5000/employee/{employee_id} in the request URL field. (192.168.1.101 for LAN connection)

20. Go to the "Body" tab.

21. Select the "raw" option.

22. Choose "JSON" from the dropdown menu next to the raw option.

23. In the request body, enter the JSON data for the employee you want to update （without employee_id）
    {
        "employee": "Alan",
        "gender": "M",
        "age": 32,
        "salary": 3000,
        "town": "Punggol"
    }


24. Click the "Send" button to send the DELETE request.

### _Note_
1. Assuming that you have already set up a basic development environment and docker desktop on your workstation.
2. This setup is for showcase purpose, not for production deployment
3. Due to Postgres database do not support replace table, the API project could only be triggered once due to the data within the databases could not be duplicated as the setting for the tables creation. I do not put drop tables tasks in the DAG due to time constraint, but you could drop the tables manually in the Postgres database if you want to retrigger the DAG again.

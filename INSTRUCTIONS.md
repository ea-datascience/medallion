## Objective
The goal of this coding exercise is to assess your technical skills in data engineering, particularly in areas such as data ingestion, processing, and basic infrastructure setup using Python. 
This exercise also aims to evaluate your ability to design and implement scalable data pipelines, as well as your familiarity with cloud platforms mentioned in the job description.

## Task Description
You are tasked with designing a simple data pipeline that simulates the ingestion, processing, and storage of data from a data source (provided in the data/customers.json file) into a data lake structure. 
Your pipeline should be able to handle JSON data, perform basic transformations, and store the processed data in a structured format suitable for analytics and machine learning purposes.

## Requirements
### Data Ingestion
* Write a Python script that loads data from the source data file.

### Data Processing
* Implement data transformations to clean and structure the data. This could include tasks such as filtering irrelevant records and extracting key information.
* Ensure the transformation logic is modular and easily extendable for future data sources or transformation requirements.

### Data Storage
* Store the processed data into a simplified data lake structure. For simplicity, you can save the data in CSV files in a local directory structure that mimics a data lake layout (e.g., organized by date or data source).

### Infrastructure as Code (Optional)
* Use Terraform to script a basic cloud storage resource where the data would be stored if this were a real environment. You can use Azure Blob Storage as the target service. Include instructions on how to deploy the Terraform script. (This part is optional and for candidates comfortable with Terraform and Azure).

## Instructions to the Candidate
* Your solution should be implemented in Python.
* Focus on writing clean and simple code. You do not need to consider scalability at this stage, simplicity is preferred.
* Your code should have tests. TDD approach is preferred.
* Include a README file with clear instructions on how to run your script, any dependencies needed, and a brief explanation of your design choices.
* If you choose to include the optional Infrastructure as Code component, ensure the README also contains instructions for setting up the cloud resources using Terraform.
* Remember, the goal is not just to complete the task but to demonstrate how you approach problem-solving, code organization, and system design.

There’s no time limit for this task, but we expect it to take less than 2 hours.

Please avoid:

* layers of abstraction
* patterns
* custom test frameworks
* architectural features that aren’t called for

### Software prerequisites

The exercise requires docker in order to run, so please make sure you have this installed.

### Solution

You can make any changes you like, as long as the solution can still be executed using the supplied Makefile. 
We’ll use the Makefile to review your solution.

To view the targets supported by the Makefile please execute make help target.

## Begin the task

There are two requirements for the task.

1. Create a process to ingest, transform, and save the data.
2. Output the top 3 customers based on total_spent descending. The output should contain the name, email and total_spent.

## Other Requirements

Please include instructions about your strategy and important decisions you made in the README file. 
You should also include answers to the following questions:

1. What kind of data quality measures would you apply to your solution in production?
2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?

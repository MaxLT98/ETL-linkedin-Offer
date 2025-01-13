from prefect import flow
from tasks.task_extract_linkedin import task_extract_linkedin
from tasks.task_load_linkedin import task_load_linkedin

@flow(name="ETL linkedin")
def main_flow():
    extracted_data = task_extract_linkedin()
    print(extracted_data)
    task_load_linkedin(extracted_data)
     
if __name__ == "__main__":
    main_flow()
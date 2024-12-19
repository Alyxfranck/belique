import requests
import json
import logging
import time
from datetime import datetime, timezone
import os
import re 
# Set up logging to output to both console and file
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Set the logging level as needed

# Create handlers
console_handler = logging.StreamHandler()  # Console handler
log_directory = 'logs'
os.makedirs(log_directory, exist_ok=True)
file_handler = logging.FileHandler(os.path.join(log_directory, 'scraper_log.log'), encoding='utf-8')  # File handler

# Set levels for handlers
console_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.ERROR)

# Create formatter and add it to handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Constants
SUBMIT_URL = "http://localhost:8000/api/submit-scrape-job"
STATUS_URL_TEMPLATE = "http://localhost/api/job/{}"
AUTH_URL = "http://localhost/api/auth/token"  # Adjusted to the correct authentication endpoint
INDEX_TRACK_FILE = 'index.json'

# Variables
output_data = []
token = None

# Load or initialize the last processed index
def load_last_processed_index():
    try:
        with open(INDEX_TRACK_FILE, 'r') as file:
            index_data = json.load(file)
            print(f"Loaded index data: {index_data}") 
            return index_data.get("index", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        print("No index data found.")
        return 0

# Save the last processed index and log it
def save_last_processed_index(index):
    index_file_path = os.path.join(log_directory, INDEX_TRACK_FILE)
    with open(index_file_path, 'w') as file:
        json.dump({"index": index}, file)
    logger.info(f"Last processed index saved: {index}")

# Function to submit a scraping job
def submit_scraping_job(url_to_scrape):
    headers = {"Authorization": f"Bearer {token}"}
    job_data = {
        "id": "",
        "url": url_to_scrape,
        "elements": [
            {
               "name": "BuisnessName",
               "xpath": '//*[@id="wp--skip-link--target"]/div[1]/div/h1',
               "url": url_to_scrape
            },
            {
               "name": "employes",
               "xpath": '//*[@id="wp--skip-link--target"]/div[1]/div/div/div[1]',
               "url": url_to_scrape
            },
            {
               "name": "Address",
               "xpath": '//*[@id="wp--skip-link--target"]/div[2]/div[3]/div[1]/div',
               "url": url_to_scrape
            },
            {
               "name": "Contact_Name",
               "xpath": '//*[@id="wp--skip-link--target"]/div[2]/div[3]/div[2]/div/p',
               "url": url_to_scrape
            },
            {
               "name": "Contact_phone",
               "xpath": '//*[@id="wp--skip-link--target"]/div[2]/div[3]/div[2]/div/div[1]/p',
               "url": url_to_scrape
            },
            {
               "name": "Contact_email",
               "xpath": '//*[@id="wp--skip-link--target"]/div[2]/div[3]/div[2]/div/div[2]/p/span',
               "url": url_to_scrape
            },
            {
               "name": "Contact_Website",
               "xpath": '//*[@id="wp--skip-link--target"]/div[2]/div[3]/div[2]/div/div[3]/p',
               "url": url_to_scrape
            },
            

        ],
        "user": "",
        "time_created": datetime.now(timezone.utc).isoformat(),
        "result": [],
        "job_options": {
            "multi_page_scrape": False,
            "custom_headers": {}
        },
        "status": "Queued",
        "chat": ""
    }
    
    try:
        response = requests.post(SUBMIT_URL, json=job_data, headers=headers)
        response.raise_for_status()
        job_id = response.json().get("id")
        
        if job_id:
            logger.info(f"Successfully submitted job for, Job ID: {job_id}")
            return job_id
        else:
            logger.error(f"No job ID returned in response for ")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to submit job for : {e}")
    
    return None

# Function to check the status of a job with added verification for list response
def check_job_status(job_id):
    headers = {"Authorization": f"Bearer {token}"}
    status_url = STATUS_URL_TEMPLATE.format(job_id)
    
    while True:
        try:
            response = requests.get(status_url, headers=headers)
            response.raise_for_status()
            job_info = response.json()
            logger.debug(f"Job info for ID {job_id}: ")

            if isinstance(job_info, list):
                if job_info and isinstance(job_info[0], dict):
                    job_status = job_info[0].get("status")
                    result = job_info[0].get("result", [])
                else:
                    logger.warning(f"Unable to extract status information from list response for Job ID {job_id}")
                    return None
            elif isinstance(job_info, dict):
                job_status = job_info.get("status")
                result = job_info.get("result", [])
            else:
                logger.error(f"Unexpected format for Job ID {job_id}: {type(job_info)}")
                return None
            
            if job_status == "Completed":
                logger.info(f"Job ID {job_id} completed.")
                return result
            elif job_status == "Failed":
                logger.error(f"Job ID {job_id} failed.")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to check status for Job ID {job_id}: {e}")
        
        time.sleep(1)
def process_result(result, url):
    # Default values for missing data
    business_name_data = "No Business Name available"
    employees_data = "No Employees available"
    address_text = "No address available"
    contact_name = "No contact name available"
    contact_phone = "No contact phone available"
    contact_email = "No contact email available"
    contact_website = "No contact website available"

    if result:
        if isinstance(result, list):
            # Iterate over the list and look for matching URLs
            for item in result:
                if isinstance(item, dict) and url in item:
                    url_data = item[url]
                    
                    # Extract BusinessName data
                    business_name_data = extract_text(url_data.get("BuisnessName", []), business_name_data)
                    
                    # Extract Employees data
                    employees_data = extract_text(url_data.get("employes", []), employees_data)
                    
                    # Extract Address data
                    address_text = extract_text(url_data.get("Address", []), address_text)
                    
                    # Extract Contact Name
                    contact_name = extract_text(url_data.get("Contact_Name", []), contact_name)
                    
                    # Extract Contact Phone
                    contact_phone = extract_text(url_data.get("Contact_phone", []), contact_phone)
                    
                    # Extract Contact Email
                    contact_email = extract_text(url_data.get("Contact_email", []), contact_email)
                    
                    # Extract Contact Website
                    contact_website = extract_text(url_data.get("Contact_Website", []), contact_website)

    # Append the extracted information to the output data
    contact_info = {
        "business_name": business_name_data,
        "employees": employees_data,
        "address": address_text,
        "contact_name": contact_name,
        "contact_phone": contact_phone,
        "contact_email": contact_email,
        "contact_website": contact_website,
    }
    output_data.append(contact_info)
    logger.info(f"Data saved for {url}: {contact_info}")

def extract_text(data_list, default_value):
    """
    Helper function to extract text from a list of dictionaries or strings and clean it.
    """
    if data_list:
        first_element = data_list[0]
        if isinstance(first_element, dict):
            text = first_element.get("text", default_value)
        elif isinstance(first_element, str):
            text = first_element
        else:
            text = default_value
        
        # Clean up text by stripping and replacing unnecessary whitespaces
        return ' '.join(text.split())
    return default_value




# Main execution logic
try:
    # Load URLs from the JSON file and the last processed index
    url_file_path = 'idea.json'
    with open(url_file_path, 'r', encoding='utf-8') as file:
        urls = json.load(file)
    
    last_index = load_last_processed_index()

    # Process each URL starting from the last processed index
    for i in range(last_index, len(urls)):
        url = urls[i]
        job_id = submit_scraping_job(url)
        
        if job_id:
            result = check_job_status(job_id)
            logger.debug(f"Scraped result for url: {url}, result: {result}")
            process_result(result, url)
            save_last_processed_index(i + 1)  # Save index after processing the URL
            
            # Save the extracted data to a JSON file after each URL is processed
            output_directory = 'data'
            os.makedirs(output_directory, exist_ok=True)
            with open(os.path.join(output_directory, "contact_data.json"), "w", encoding='utf-8') as outfile:
                json.dump(output_data, outfile, indent=4, ensure_ascii=False)
            logger.info("Data saved to contact_data.json")
            
            time.sleep(1)  # Short delay between jobs
        else:
            logger.error(f"Skipping job for {url} due to submission failure.")

finally:
    logger.info("Scraping complete.")

    
   
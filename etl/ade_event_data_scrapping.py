from selenium import webdriver
import re
import time
import pandas as pd
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import boto3

# AWS S3 Configuration
AWS_S3_BUCKET_NAME = 'ade-data-bucket'
AWS_REGION = 'eu-north-1'
AWS_ACCESS_KEY = 'AKIAWPPO6UKMRRHGIS5A'
AWS_SECRET_KEY = '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'

def initialize_driver():
    """
    Initialize and return a Selenium WebDriver instance for Firefox.

    Returns:
        webdriver.Firefox: A Firefox WebDriver instance.
    """
    options = Options()
    options.set_preference("dom.webdriver.enabled", False)
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)
    return driver

def handle_cookie_consent(driver):
    """
    Handle the cookie consent popup if it appears.

    Args:
        driver (webdriver.Firefox): The WebDriver instance.
    """
    try:
        consent_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'fc-button.fc-cta-consent.fc-primary-button'))
        )
        consent_button.click()
        print("Successfully consented.")
        WebDriverWait(driver, 5).until(EC.invisibility_of_element(consent_button))
    except Exception as e:
        print("The consent button was not found or it was already accepted:", e)

def scrape_event_urls(driver, base_url):
    """
    Scrape event URLs from the main agenda page.

    Args:
        driver (webdriver.Firefox): The WebDriver instance.
        base_url (str): The base URL of the website.

    Returns:
        list: A list of event URLs.
    """
    driver.get(base_url)
    agenda_html = driver.page_source
    agenda_soup = BeautifulSoup(agenda_html, "html.parser")
    events = agenda_soup.find_all("a", class_="list-group-item agendaitem")
    events_href = [event["href"] for event in events if event["href"].startswith("/party.p")]
    return events_href

def scrape_event_details(driver, event_url):
    """
    Scrape detailed information about a specific event.

    Args:
        driver (webdriver.Firefox): The WebDriver instance.
        event_url (str): The URL of the event to scrape.

    Returns:
        dict: A dictionary containing event details.
    """
    driver.get(event_url)
    event_html = driver.page_source
    event_soup = BeautifulSoup(event_html, "html.parser")
    event_info = event_soup.find("div", id="eventinfo")

    event_name_div = event_info.find("div", class_="titlewithnav")
    event_name = event_name_div.text if event_name_div else None

    event_start_meta = event_info.find("meta", itemprop="startDate")
    event_start = event_start_meta["content"] if event_start_meta else None

    event_end_meta = event_info.find("meta", itemprop="endDate")
    event_end = event_end_meta["content"] if event_end_meta else None

    event_location_span = event_info.find("div", itemprop="location").find("span", itemprop="name")
    event_location = event_location_span.text if event_location_span else None

    event_address_span = event_info.find("span", itemprop="streetAddress")
    event_adress = event_address_span.text if event_address_span else None

    event_locality_span = event_info.find("span", itemprop="addressLocality")
    event_locality = event_locality_span.text if event_locality_span else None

    event_country_span = event_info.find("span", itemprop="addressRegion")
    event_country = event_country_span.text if event_country_span else None

    event_latitude_meta = event_info.find("meta", itemprop="latitude")
    event_latitude = event_latitude_meta["content"] if event_latitude_meta else None

    event_longitude_meta = event_info.find("meta", itemprop="longitude")
    event_longitude = event_longitude_meta["content"] if event_longitude_meta else None

    event_capacity_div = event_info.find_all("div", class_="table-partydetail")[1].find("div", class_="value-partydetail")
    event_capacity = re.sub(r'\D', '', event_capacity_div.text) if event_capacity_div else None

    event_price_div = event_info.find("div", itemprop="offers")
    event_price = event_price_div.text if event_price_div else None

    event_genre_div = event_info.find("div", class_="label-partydetail", title="Genre indication")
    event_genre = event_genre_div.find_next_sibling("div", class_="value-partydetail").text if event_genre_div else None

    event_lineup_div = event_info.find("div", class_="lineup-partydetail").find("div") if event_info.find("div", class_="lineup-partydetail") else None
    event_lineup = event_lineup_div.text if event_lineup_div else None

    event_state_span = event_info.find("span", class_="red smallfont")
    event_state = event_state_span.text if event_state_span else None

    event_dict = {
        "name": event_name,
        "start_date": event_start,
        "end_date": event_end,
        "location": event_location,
        "address": event_adress,
        "locality": event_locality,
        "country": event_country,
        "latitude": event_latitude,
        "longitude": event_longitude,
        "capacity": event_capacity,
        "price": event_price,
        "genre": event_genre,
        "lineup": event_lineup,
        "state": event_state
    }

    return event_dict

def save_to_csv(event_list, year):
    """
    Save the list of events to a CSV file.

    Args:
        event_list (list): A list of event dictionaries.
        year (int): The year of the events.

    Returns:
        str: The name of the saved CSV file.
    """
    df_events = pd.DataFrame(event_list)
    file_name = f'ade_events_{year}.csv'
    df_events.to_csv(file_name, index=False)
    print(f"DataFrame has been saved to {file_name}.")
    return file_name

def upload_to_s3(file_name, bucket_name, region_name, access_key, secret_key):
    """
    Upload a file to an AWS S3 bucket.

    Args:
        file_name (str): The name of the file to upload.
        bucket_name (str): The name of the S3 bucket.
        region_name (str): The AWS region.
        access_key (str): The AWS access key.
        secret_key (str): The AWS secret key.
    """
    s3_client = boto3.client(
        service_name='s3',
        region_name=region_name,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    response = s3_client.upload_file(file_name, bucket_name, file_name)
    print(f'Upload to AWS S3 response: {response}')

def main():
    """
    Main function to scrape event data, save it to a CSV file, and upload it to AWS S3.
    """
    year = 2005
    base_url = f"https://www.djguide.nl/events.p/ade/{year}?language=en"

    driver = initialize_driver()
    handle_cookie_consent(driver)
    events_href = scrape_event_urls(driver, base_url)

    event_list = []
    for i, event_href in enumerate(events_href):
        print(f"Processing event {i + 1}/{len(events_href)}")
        event_url = f"https://www.djguide.nl{event_href}"
        event_details = scrape_event_details(driver, event_url)
        event_list.append(event_details)

    file_name = save_to_csv(event_list, year)
    upload_to_s3(file_name, AWS_S3_BUCKET_NAME, AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY)

    driver.quit()

if __name__ == '__main__':
    main()
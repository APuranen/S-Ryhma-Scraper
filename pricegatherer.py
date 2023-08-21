import os.path
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import pytz
import csv
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from unidecode import unidecode

# GLOBALS
BASE_DIR = os.path.dirname((os.path.abspath(__file__)))
PRODUCTS_SRYHMA = os.path.join(BASE_DIR, "seurattavat_tuotteet_SRYHMA.csv")
TEST = True


def sryhma_scraper(logger):
    data = []
    with open(PRODUCTS_SRYHMA, 'r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=";")

        for row in reader:
            link, category = row
            try:
                response = requests.get(link)
                content = response.content

                soup = BeautifulSoup(content, "html.parser")

                product_name_elem = soup.find('h1', {'data-test-id': 'product-name'})
                unit_price_elem = soup.find('span', {'data-test-id': 'product-price__unitPrice'})
                comparison_price_elem = soup.find('div', {'data-test-id': 'product-page-price__comparisonPrice'})

                product_name = unidecode(product_name_elem.text.strip()) if product_name_elem else ''
                product_price = unit_price_elem.text if unit_price_elem else ''
                comparison_price = comparison_price_elem.text if comparison_price_elem else ''

            except Exception as e:
                logger.error(f"Exception occured when scraping link {link}", exc_info=True)

            timestamp = get_timestamp(True)
            data.append({
                'Timestamp': timestamp,
                'ChainID': 'SRyhma',
                'ProductClass': category,
                'ProductID': product_name,
                'ProductPrice': product_price,
                'ProductComparisonPrice': comparison_price
            })

    return data


def validate_and_clean_data(df):

    total_rows = len(df)
    df.replace('', np.nan, inplace=True)
    missing_rows = df['ProductID'].isnull()
    num_missing_rows = missing_rows.sum()
    missing_row_numbers = [i + 2 for i, val in enumerate(missing_rows) if val]

    with open(PRODUCTS_SRYHMA, 'r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=";")
        links = [row[0] for row in reader]

    dead_links = [links[i - 1] for i in missing_row_numbers]

    console_log("Data validation complete.")

    console_log(f"{total_rows - num_missing_rows} / {total_rows} rows were succesfully scraped")
    if num_missing_rows > 0:
        console_log(f"{num_missing_rows} row(s) are missing values.")
        console_log(f"Row numbers with missing values: {missing_row_numbers}")
        console_log(f"Dead links: {dead_links}")
        df.dropna(subset=['ProductID'], inplace=True)
        console_log(f"Dropped {num_missing_rows} rows with erroneous values.")

    df['ProductID'] = df['ProductID'].str.replace(',', '.', regex=False)

    df['ProductPrice'] = df['ProductPrice'].str.replace(r'[~€"\s]', '', regex=True)
    df['ProductPrice'] = df['ProductPrice'].str.replace(',', '.', regex=False)

    df['ProductComparisonPrice'] = df['ProductComparisonPrice'].str.replace(r'[~€"\s]', '', regex=True)
    df['ProductComparisonPrice'] = df['ProductComparisonPrice'].str.replace(',', '.', regex=False)

    return df


def run_scraper(logger):
    data = []
    console_log("Running scraper.")

    try:
        data_sryhma = sryhma_scraper(logger)
        data.append(pd.DataFrame(data_sryhma))
    except Exception as e:
        console_log("Exception occured when scraping for S-Ryhma data, check log for more details")
        logger.error("Exception occured when running S-Ryhma Scraper", exc_info=True)

    df = pd.concat(data, ignore_index=True)
    df = validate_and_clean_data(df)

    filename = 'output_' + get_timestamp() + '.csv'
    df.to_csv(filename, index=False)


def console_log(message):
    print(f"[{get_timestamp(True)}] - {message}")


def get_timestamp(seconds=False):
    timezone = pytz.timezone("Etc/GMT-3")
    if seconds:
        timestamp = datetime.now(timezone).strftime('%Y-%m-%d - %H:%M:%S')
    else:
        timestamp = datetime.now(timezone).strftime('%Y-%m-%d')
    return timestamp


def main():
    logger = logging.getLogger("scraper_logger")
    logger.setLevel(logging.ERROR)

    handler = logging.FileHandler("log.txt")
    handler.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    print("Initializing scheduler")
    scheduler = BlockingScheduler()
    scheduler.add_job(run_scraper, 'cron', hour=0, args=[logger], misfire_grace_time=20)

    if TEST:
        console_log("Testing active, running scrapers immediately")
        run_scraper(logger)

    console_log("Scheduler started, running scrapers periodically at midnight.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        console_log("\nStopping scraper and exiting.")
        scheduler.shutdown()


if __name__ == '__main__':
    main()

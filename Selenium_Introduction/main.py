import time
import os
import pandas as pd
import csv

import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.edge.service import Service as EdgeService

class SeleniumWebDriverContextManager:
    """
    Context Manager for Selenium WebDriver, to setup WebDriver instance.
    Supports Chrome, Firefox, and Edge.
    """
    def __init__(self, browser='chrome', driver_path=None, options=None):
        self.browser = browser.lower()
        self.driver_path = driver_path
        self.options = options
        self.driver = None

    def __enter__(self):
        if self.browser == 'chrome':
            opts_chr = self.options or webdriver.ChromeOptions()
            opts_chr.add_argument('--headless')
            service = ChromeService(executable_path=self.driver_path) if self.driver_path else ChromeService()
            self.driver = webdriver.Chrome(service=service, options=opts_chr)
        elif self.browser == 'firefox':
            opts_frx = self.options or webdriver.FirefoxOptions()
            opts_frx.add_argument('--headless')
            service = FirefoxService(executable_path=self.driver_path) if self.driver_path else FirefoxService()
            self.driver = webdriver.Firefox(service=service, options=opts_frx)
        elif self.browser == 'edge':
            opts_edge = self.options or webdriver.EdgeOptions()
            opts_edge.add_argument('--headless')
            service = EdgeService(executable_path=self.driver_path) if self.driver_path else EdgeService()
            self.driver = webdriver.Edge(service=service, options=opts_edge)
        else:
            raise ValueError(f"Provided browser is not supported: {self.browser}")
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Quit WebDriver on exit.
        """
        if self.driver:
            self.driver.quit()

def extract_svg_table(driver, html_path, output_dir=".", num_columns=3):
    """
    Extracts data from SVG/HTML text elements with class 'cell-text' and saves as CSV.
    Handles column-major grouping with header at the end of each column.
    """
    driver.get(f"file://{html_path}")
    wait = WebDriverWait(driver, 10)
    os.makedirs(output_dir, exist_ok=True)
    table_csv_path = os.path.join(output_dir, "table_extract.csv")

    try:
        # Find all cell-text elements
        cells = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "text.cell-text")))
        cell_texts = [cell.text for cell in cells]

        # Calculate number of rows per column
        total_cells = len(cell_texts)
        rows_per_col = total_cells // num_columns

        # Split into columns
        columns = [cell_texts[i*rows_per_col:(i+1)*rows_per_col] for i in range(num_columns)]

        # Extract headers (last item in each column)
        headers = [col[-1] for col in columns]
        # Remove header from each column
        columns = [col[:-1] for col in columns]

        # Transpose columns to rows
        rows = list(zip(*columns))

        with open(table_csv_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"Extracted SVG table data saved to {table_csv_path}")
    except Exception as e:
        print(f"Error extracting SVG table data: {e}")


def extract_doughnut_chart(driver, html_path, output_dir="."):
    driver.get(f"file://{html_path}")
    wait = WebDriverWait(driver, 10)
    screenshot_idx = 0

    os.makedirs(output_dir, exist_ok=True)

    try:
        # Find all doughnut chart segments
        paths = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "path.surface")))
        # Screenshot
        driver.save_screenshot(os.path.join(output_dir, f"screenshot{screenshot_idx}.png"))
        # Extract chart data
        chart_data = []
        for idx, path in enumerate(paths):
            d_attr = path.get_attribute("d")
            color = path.value_of_css_property("fill")
            chart_data.append([f"segment_{idx}", d_attr, color])
        # Save chart data
        doughnut_csv_path = os.path.join(output_dir, f"doughnut{screenshot_idx}.csv")
        with open(doughnut_csv_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Segment", "SVG Path", "Color"])
            writer.writerows(chart_data)
        print(f"Saved screenshot and chart data to {doughnut_csv_path}")
    except TimeoutException as e:
        print(f"Could not locate doughnut chart segments: {e}")

def save_chart_data(driver, idx, output_dir="."):
    """
    Extracts doughnut chart data and saves it as a CSV file in output_dir.
    """
    try:
        chart_data = []
        chart_table = driver.find_element(By.CSS_SELECTOR, "#chart-data-table")
        rows = chart_table.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            chart_data.append([col.text for col in cols])
        doughnut_csv_path = os.path.join(output_dir, f"doughnut{idx}.csv")
        with open(doughnut_csv_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerows(chart_data)
    except (NoSuchElementException, Exception) as e:
        print(f"Could not extract chart data for doughnut{idx}: {e}")

if __name__ == "__main__":
    file_path = "/Users/tomasz_tokarzewski/Documents/DQE_repository/dqe-automation/Selenium_Introduction/report_from_container.html"
    browser = 'chrome'
    driver_path = None
    output_dir = "/Users/tomasz_tokarzewski/Documents/DQE_repository/dqe-automation/Selenium_Introduction/output"  # <-- your desired output path

    with SeleniumWebDriverContextManager(browser=browser, driver_path=driver_path) as driver:
        extract_svg_table(driver, file_path, output_dir, num_columns=3)
        extract_doughnut_chart(driver, file_path, output_dir)
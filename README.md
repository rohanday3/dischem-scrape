# Dischem Scraper

## Description

The Dischem Scraper is a Python web scraping tool that gathers product information from the Dischem website. It allows you to extract product details, such as descriptions, schedules, Nappi codes, barcodes, and prices, from various categories.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Installation

To use the Dischem Scraper, follow these installation steps:

1. Clone the repository:
   ```shell
   git clone https://github.com/rohanday3/dischem-scrape/
   ```
2. Install the required Python packages:
   ```shell
   pip install aiohttp beautifulsoup4 tqdm pandas matplotlib
   ```
3. Run the scraper:
   ```shell
   python main.py
   ```

### Usage
To use the Dischem Scraper:

Specify the categories you want to scrape by modifying the categories list in scrape.py.

Run the scraper using the command mentioned in the installation section.

The scraper will collect product information from the Dischem website and save it to a CSV file.

### Features
Web scraping of product information from Dischem's website.
Customizable category selection.
Data is saved to a CSV file for further analysis.
Contributing
If you'd like to contribute to this project, please follow these guidelines:

Fork the repository.
Create a new branch.
Make your changes.
Commit your changes with clear and concise messages.
Push your changes to your branch.
Create a pull request.

### License
This project is licensed under the MIT License. You can find the full license text in the LICENSE file.

### Contact
For any questions or suggestions, feel free to contact the project author:<br>
Rohan Dayaram<br>
Email: rohanday4@gmail.com<br>

### Acknowledgments
This project was inspired by the need for a tool to gather product information from Dischem for various analytical purposes.

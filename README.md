# Wine Research Project

This project aims to analyze and visualize data related to the wine industry utilizing data-driven approaches to forecast wine prices. It involved collecting data from various sources (FRED, USITC, TTB) and leveraging them in state space model to forecast various datapoints. Dockerized for easy collaboration and reproducibility, the project automates data updates using an Airflow DAG pipeline. Feel free to use this to explore the world of wine forecasting.

## Table of Contents

- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Contributing](#contributing)
- [License](#license)

## Project Structure

The project directory structure is organized as follows:
```
wine_research/
├── data/
│ ├── archive_data/
│ ├── FRED/
│ ├── TTB/
│ └── USITC/
└── src/
│ ├── apis/
│ ├── data_ingestion/
│ ├── data_modeling/
│ ├── data_visualization/
│ ├── scripts/
│ ├── sql/
└─└── web/
```

- `data/`: Contains various data directories including raw data, archive data, and data from different sources.
- `src/`: Contains source code for data ingestion, procession, (TODO) modeling, (TODO) visualization, (TODO) SQL queries, and the web application.

## Installation

To set up the project, follow these steps:

1. Clone the repository:
```
bash
git clone <repository-url>
cd wine_research
```

2. Set up the environment:
    1. Create a virtual environment (recommended) and activate it.
    2. Install the required dependencies by running:
    ```
    pip install -r requirements.txt
    ```

3. Set up the database:
* Make sure Docker and Docker Compose are installed on your system.
    1. Create an `.env` file and configure the necessary environment variables
    2. Build and start the containers using Docker Compose:
    ```
    docker-compose up -d --build
    ```
* This should automatically run the ingestion tasks. It takes several minutes to wrap up on my machine.

4. Navigate to http://localhost:8080 to see the application run in the browser
* You'll be able to see what the routes that serve the web vs routes that pass along data in `src/apis/winetariffs_api.py`. Feel free to navigate to any of those routes and collect whatever data pleases you.

## Usage

Once this project is set up, you can perform the following tasks:
* Modify and run the data ingestion scripts to update the database with new data.
* Query out the database various panel data and time series datapoints
* TODO: Utilize the data processing scripts to analyze and process the collected data.
* TODO:Use the data visualization scripst to create visualizations and gain insights from the data
* Customize the web application located in `src/web` to provide an interactive interface for exploring the data


## Data Sources

The project utilizes data from the following sources:
* FRED (Federal Reserve Economic Data)
* Alcohol and Tobacco Tax and Trade Bureau (TTB)
* United States International Trade Commission (USITC)


## License

This project is licensed under the MIT License.

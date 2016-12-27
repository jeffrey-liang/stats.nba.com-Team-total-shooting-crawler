#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict, deque
import sqlite3
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd


def generate_links(url, start, end):
    '''
    Generate links.

    Parameters:
    -----------
    start: int
        The starting year.
    end: int
        The ending year

    Returns:
    --------
    years: collections.OrderedDict()
        Links to be crawled. 
    '''

    years = OrderedDict()
    start_dates = pd.date_range(start=str(start), end=str(end), freq='A')
    end_dates = pd.date_range(start=str(start + 1), end=str(end + 1), freq='A')

    for start, end in zip(start_dates.year, end_dates.year):

        # Generating dates in the format '2000-01'.
        ending = '{:02d}'.format(end % 100)
        year = '{}-{}'.format(start, ending)
        link = url.format('{}-{}'.format(start, ending))

        years[year] = link

    return years


def scrape_table(html, columns=None):
    '''
    Scrape the data table of Team Shooting.

    Parameters:
    -----------
    html: str
        The page source.
    columns: list, optional
        The columns of the to be scraped table.

    Returns:
    --------
    data: collections.OrderedDict()
        The table and the year as key.
    '''

    bs_obj = BeautifulSoup(html, 'lxml')

    table_data = OrderedDict()
    table = bs_obj.find_all(
        'div', {'class': 'nba-stat-table'})[0].find_all('tbody')

    for row in table[0].find_all('tr'):
        team = None
        team_stats = deque()

        for data in row.find_all('td'):

            if (data['class'][0] == 'first'):
                team = data.get_text()

            else:
                team_stats.append(data.get_text())

            table_data[team] = team_stats

    if columns is not None:
        data = pd.DataFrame.from_dict(table_data, orient='index')
        data.columns = columns

    return data


def add_two_cols(data, season):
    '''
    After scraping the html and acquiring the table using 'scrape_table()' 
    method, add the season and team columns to the beginning of the table.

    Parameters:
    -----------
    data: pandas.core.DataFrame
        The table with the data.
    season: str
        The season of the table.

    Returns:
    --------
    data: pandas.core.DataFrame
        The updated data table.
    '''

    data['Season'] = season
    data['Team'] = data.index

    # Move the last two columns (i.e. season and team) to the beginning.
    cols = data.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    data = data[cols]

    return data


def save_to_sqlite3(data, db_name, table_name, season):
    '''
    Save the dataframe into Sqlite3 database.

    Parameters:
    -----------
    data: pandas.core.DataFrame
        The data to be saved.
    db_name: str
        The database name.
    table_name: str
        The table name.

    Returns:
    --------
    None
    '''

    data = add_two_cols(data, season)

    con = sqlite3.connect(db_name)
    data.to_sql(table_name, con, index=False,
                if_exists='replace', index_label=True)


def fetch(url):
    '''
    Fetch the url, then return the page source.

    Parameters:
    -----------
    url: str
        The link of page to fetch.

    Returns:
    --------
    The page source of the url.
    '''
    driver = webdriver.PhantomJS('/usr/local/bin/phantomjs')
    driver.get(url)
    page_source = driver.page_source
    driver.close()

    return page_source


def crawler(urls, db_name, columns=None):
    '''
    Main method to scrape the urls for data.

    Parameters:
    -----------
    urls: list
        The list of urls to crawl.
    db_name: str
        Name of sqlite3 database.
    columns: list. optional
        The columns of the to be scraped table.
    '''
    # Store all the links that failed
    missed_links = OrderedDict()

    for season, url in urls.items():

        print('Fetching: {} Totals'.format(season))

        try:

            page_source = fetch(url)

            print('Saving to sqlite: {}'.format(season))
            table_name = '{}_totals'.format(season)
            save_to_sqlite3(scrape_table(page_source, columns=columns),
                            db_name, table_name, season)

        except Exception as e:
            missed_links[season] = url
            print('ERROR: {} on {}'.format(e, url))

    # Try to fetch error links again.
    if missed_links:
        for season, url in missed_links.items():

            print('Fetching: {} Totals (Again)'.format(season))
            try:
                page_source = fetch(url)

                print('Saving to sqlite: {}'.format(season))
                table_name = '{}_totals'.format(season)
                save_to_sqlite3(scrape_table(page_source, columns=columns),
                                db_name, table_name, season=season)

            except Exception as e:
                print('ERROR: {} on {}.\nFailed twice.'.format(e, url))

url = 'http://stats.nba.com/teams/shooting/#!?sort=5-9%20ft.%20FG%20PCT&%5C%20dir=1&Season={}&SeasonType=Regular%20Season&PerMode=Totals'


columns = ['<5 FGM', '<5 FGA', '<5 FG%',
           '5-9 FGM', '5-9 FGA', '5-9 FG%',
           '10-14 FGM', '10-14 FGA', '10-14 FG%',
           '15-19 FGM', '15-19 FGA', '15-19 FG%',
           '20-24 FGM', '20-24 FGA', '20-24 FG%',
           '25-29 FGM', '25-29 FGA', '25-29 FG %']

urls = generate_links(url, 1997, 2017)
crawler(urls, 'team_shooting.db', columns=columns)

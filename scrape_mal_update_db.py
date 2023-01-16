import os
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values
from datetime import date, timedelta

def save_people(people_data):
  try:
    connection = psycopg2.connect(user=os.getenv("MAL_PEOPLE_USER"),
                                  password=os.getenv("MAL_PEOPLE_PASSWORD"),
                                  host=os.getenv("MAL_PEOPLE_HOST"),
                                  port=os.getenv("MAL_PEOPLE_PORT"),
                                  database=os.getenv("MAL_PEOPLE_DB"))
    cursor = connection.cursor()
    execute_values(cursor,
      "insert into people(person_id_date, person_id, date, english_name, japanese_name, mal_link, image_link, favorites) values %s on conflict do nothing;",
      people_data)
    connection.commit()
  except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
  finally:
    if (connection):
      cursor.close()
      connection.close()

def search_page(limit, today):
  url = "https://myanimelist.net/people.php?limit=" + str(limit)
  page = requests.get(url)

  mal_people_soup = BeautifulSoup(page.content, "html.parser")

  people_rows = mal_people_soup.find_all("tr", class_="ranking-list")

  people_data = []
  # just some number larger than any persons favorites so the min operation always takes the first encountered
  max_least_favorites = 1000000000
  least_favorites_on_page = max_least_favorites
  for person_row in people_rows:
    try:
      id_element = person_row.find("a", class_=["fl-l", "ml12", "mr8"])
      link = id_element.get("href")
      id = link.split("/")[4]

      image_element = person_row.find("img")
      image_link = image_element.get("data-src")

      english_name_element = person_row.find("a", class_=["fs14", "fw-b"])
      english_name = english_name_element.text.strip()

      japanese_name_element = person_row.find("span", class_=["fs12", "fn-grey6"])
      if japanese_name_element:
        japanese_name = japanese_name_element.text.strip().replace("(", "").replace(")", "")
      else:
        japanese_name = ""

      favorites_count = int(person_row.find("td", class_="favorites").text.strip().replace(",", ""))

      if (english_name != ""):
        id_date = str(id) + "_" + today
        people_data.append((id_date, id, today, english_name, japanese_name, link, image_link, favorites_count))

      least_favorites_on_page = min(least_favorites_on_page, favorites_count)
      if (least_favorites_on_page == max_least_favorites):
        least_favorites_on_page = 0
    except (Exception) as error:
      print("Error occured processing row")

  save_people(people_data)
  if (least_favorites_on_page == 0):
    return False
  else:
    return True

def create_favorites_change_tables(table_name, start_date, end_date):
  try:
    connection = psycopg2.connect(user=os.getenv("MAL_PEOPLE_USER"),
                                  password=os.getenv("MAL_PEOPLE_PASSWORD"),
                                  host=os.getenv("MAL_PEOPLE_HOST"),
                                  port=os.getenv("MAL_PEOPLE_PORT"),
                                  database=os.getenv("MAL_PEOPLE_DB"))
    cursor = connection.cursor()
    cursor.execute(
      f"""
        drop table if exists {table_name};

        create table {table_name} as select distinct on (j1.person_id)
        j1.person_id,
        j1.english_name,
        j1.japanese_name,
        j1.mal_link,
        j1.image_link,
        j1.old_favorite_count,
        max_f.new_favorite_count,
        (max_f.new_favorite_count - j1.old_favorite_count) change
        from
          (
            (
              select distinct
              p.person_id,
              p.english_name,
              p.japanese_name,
              p.mal_link,
              p.image_link,
              min_f.favorites old_favorite_count
              from
                people p
                join
                (
                  select
                  p_min.person_id,
                  p_min.favorites
                  from
                    people p_min
                    join
                    (
                      select
                      person_id,
                      min(date) min_date
                      from
                        (
                          select * from people p where date between '{start_date}' and '{end_date}'
                        ) min_where
                        group by person_id
                    ) min_date
                    on p_min.date = min_date.min_date and p_min.person_id = min_date.person_id
                ) min_f
                on p.person_id = min_f.person_id
              ) j1
              join
              (
                select
                p_max.person_id,
                p_max.favorites new_favorite_count 
                from
                  people p_max
                  join
                  (
                    select
                    person_id,
                    max(date) max_date
                    from
                      (
                        select * from people p where date between '{start_date}' and '{end_date}'
                      ) max_where
                      group by person_id
                  ) max_date
                  on p_max.date = max_date.max_date and p_max.person_id = max_date.person_id
              ) max_f
              on j1.person_id = max_f.person_id
          )
          order by change desc, new_favorite_count asc;
      """
    )
    connection.commit()
  except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
  finally:
    if (connection):
      cursor.close()
      connection.close()

today_date = date.today()
today_date_string = str(today_date)
limit = 0
# this less than 100000 is just done as a precaution if something weird happens to prevent an infinite loop
while limit < 100000 and search_page(limit, today_date_string):
  limit += 50

yesterday_date_string = str(today_date - timedelta(days = 1))
create_favorites_change_tables("one_day_favorite_diff", yesterday_date_string, today_date_string)

seven_days_ago_date_string = str(today_date - timedelta(days = 7))
create_favorites_change_tables("seven_day_favorite_diff", seven_days_ago_date_string, today_date_string)

thirty_days_ago_date_string = str(today_date - timedelta(days = 30))
create_favorites_change_tables("thirty_day_favorite_diff", thirty_days_ago_date_string, today_date_string)

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
import pandas as pd
import psycopg2

access_token = "EAAHxljoDo8oBAEpMn2IqAfLWFV08dhuH6SmwjIlDhYkW4eLi7nCZCFQ8SqmBlYeFy6YNZA1s0BFs4NiHXFFbWpoUOJDm6ubIsIRE3fUIuJnENpdaqIIqHm1jN0XCmaZAR9NL26o7G8bB0u4HejSsd38HezJFuzUXPnI2xMRoyCWwfZCHlnFN"
api_ac = FacebookAdsApi.init(access_token=access_token)
conn = psycopg2.connect(dbname="marketing_data", user = "postgres", host="localhost", password="password", port = "5432")
cur = conn.cursor()
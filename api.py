from pydantic import BaseModel
from fastapi import FastAPI
from expiringdict import ExpiringDict
from pyspark.sql import SparkSession

app = FastAPI()

cache = ExpiringDict(max_len=100, max_age_seconds=600)
sparkSession = SparkSession.builder.appName("hdfs-read-write").getOrCreate()

class Payload(BaseModel):
    category: str


def filter(category, article):
	if category == 'covid':
		if len([i for i in ['covid', 'коронавирус', 'пневмония', 'КВИ'] if i.lower() in article]) != 0:
			return True
	if category == 'политика':
		if len([i for i in ['политика', 'аким', 'Токаев', 'президент'] if i.lower() in article]) != 0:
			return True
	return category in article


def get_hdfs(category):
	df = sparkSession.read.parquet('hdfs://localhost:9000/news/categories.parquet')
	df.show()
	return [i.articles for i in df.collect() if category==i.category]


def get_articles():
	import requests
	from bs4 import BeautifulSoup
	text = ""

	for i in range(10):
		r = requests.get(f"https://www.nur.kz/ajax/pagination/pseudo-category/latest/{i}/", headers={'X-Requested-With' :'XMLHttpRequest'})
		text += r.text

	soup = BeautifulSoup(text, 'html.parser')

	articles = soup.find_all(class_='article-preview-category__subhead')

	results = []
	for i in articles:
		results.append(i.text)

	return results


@app.post("/hdfs")
async def get_rest_hdfs(category: Payload):
	articles = get_hdfs(category.category.lower())
	if len(articles) == 0:
		new_articles = [i for i in get_articles() if filter(category.category.lower(), i.lower())]
		df = sparkSession.createDataFrame([(category.category.lower(), new_articles)], ['category', 'articles'])
		df.show()
		df.write.mode('append').parquet('hdfs://localhost:9000/news/categories.parquet')
		articles = new_articles
	return {"news": articles}


@app.post("/news")
async def get_news(category: Payload):
	if category.category.lower() in cache:
		return {'news': cache[category.category.lower()]}
	articles = [i for i in get_articles() if filter(category.category.lower(), i.lower())]
	cache[category.category.lower()] = articles
	return {"news": articles}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app")

import json
import urllib.request
from kafka import KafkaProducer

# Configuration
API_KEY = "api-key"
NEWS_API_URL = f"https://newsapi.org/v2/top-headlines?country=us&pageSize=10&apiKey={API_KEY}"
KAFKA_BROKER = "broker-id"  # Replace with your EC2 Kafka IP
KAFKA_TOPIC = "topic"  # Replace with your Kafka topic

# Lambda Handler
def lambda_handler(event, context):
    try:
        # Fetch data from News API
        with urllib.request.urlopen(NEWS_API_URL) as response:
            data = json.loads(response.read())
            articles = data.get("articles", [])

            # Extract key fields
            news_output = []
            for article in articles:
                simplified = {
                    "title": article.get("title"),
                    "source": article.get("source", {}).get("name"),
                    "publishedAt": article.get("publishedAt"),
                    "url": article.get("url")
                }
                news_output.append(simplified)

            # Send to Kafka
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                for article in news_output:
                    producer.send(KAFKA_TOPIC, value=article)
                producer.flush()
            except Exception as kafka_err:
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": f"Kafka Error: {str(kafka_err)}"})
                }

            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "status": "success",
                    "articles_sent": len(news_output)
                })
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "error",
                "message": str(e)
            })
        }

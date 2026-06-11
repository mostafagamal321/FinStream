from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'alert-service',
    'auto.offset.reset': 'latest'
})

consumer.subscribe(['fraud_alerts'])

def route_alert(alert):
    decision = alert["decision"]
    score = alert["fraud_score"]

    if decision == "BLOCK":
        print("🚨 SMS + EMAIL + SLACK:", alert)

    elif decision == "REVIEW":
        print("⚠️ SLACK + EMAIL:", alert)

    else:
        print("ℹ️ EMAIL only:", alert)


print("Alert service started...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    alert = json.loads(msg.value().decode("utf-8"))
    route_alert(alert)
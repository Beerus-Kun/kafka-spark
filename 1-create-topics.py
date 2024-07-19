from kafka.admin import KafkaAdminClient, NewTopic

if __name__ == "__main__":
    # init
    # admin_client = KafkaAdminClient(bootstrap_server='localhost:9092', client_id = 'admin')
    admin_client = KafkaAdminClient()

    # create new topics
    # equivalent: "kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topics  --partitions 2 --replication_factor 1"
    # topics: search, click, purchase
    topic_list = []
    shop_topic = NewTopic(name='shop', num_partitions=2, replication_factor=1)
    topic_list.append(shop_topic)

    stream_topic = NewTopic(name='streaming', num_partitions=2, replication_factor=1)
    topic_list.append(stream_topic)

    admin_client.create_topics(new_topics=topic_list)
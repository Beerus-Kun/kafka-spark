import json, random
from datetime import datetime, timedelta
from time import sleep

from kafka.producer import KafkaProducer


if __name__ == "__main__":
    # create producer
    # equivalent: "kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bankbranch"
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    # producer.send('bankbranch', {'atmid':1, 'transid':100})
    # producer.send('bankbranch', {'atmid':2, 'transid':101})
    # quần, áo, váy, đồ trang điểm, giỏ xách, 
    # nhẫn, dây chuyền, vòng tay, bông tai, 
    # bờm, đồ chơi trẻ em, đồ chơi trí tuệ, đồ chơi câu đố, đồ chơi xếp hình, 
    # sạc, tai nghe, đồng hồ, chuột, bàn phím, máy tính, 
    # quạt, loa, điện thoại, drone, xe hơi, 
    # xe máy, xe máy điện, xe hơi điện, xe đạp điện, xe đạp, 
    # snack, mì, cặp bao lô, máy xấy tóc, bếp ga, 
    # bếp điện từ, bếp hồng ngoại, giày, dép, dầu gội đầu, 
    # nước lau sàn, xà bông tắm, xà bông giặt, nến thơm, máy lọc khí, 
    # khẩu trang, áo khoác, áo mưa, kem đánh răng, nón, 
    # gạo, sữa, chảo, dao, nước ngọt, 
    # bia, cà phê, trà, 
    # pant, shirt, dress, 

    # search, click, purchase
    hour = 0
    minute = 0
    num = 0
    date = datetime(2024, 7, 15)
    catagory_id = 0
    id = 0
    while True:
        sleep(1)
        customer_id = random.randint(1, 1000)
        curr_date = date.strftime('%Y-%m-%d')

        if hour <19 and hour > 2:
            num = random.randint(10, 30)
        else:
            num = random.randint(20, 50)

        for _ in range(num):
            
            catagory_id = random.randint(1, 65)
            search_rate = random.random()
            if search_rate <0.9:
                extra_minute = random.randint(0, 4)
                second = random.randint(0, 59)
                producer.send('shop', {'id': id, 
                                       'type': 'Search', 
                                       'catagory_id':catagory_id,
                                       'customer_id': customer_id, 
                                       'datetime': f'{curr_date} {hour}:{minute+extra_minute}:{second}'})
                id += 1

            num = random.randint(2, 15)

            for _ in range(num):
                sleep(0.05)
                extra_minute = random.randint(0, 4)
                second = random.randint(0, 59)
                producer.send('shop', {'id': id, 
                                       'type': 'Click', 
                                       'catagory_id':catagory_id, 
                                       'customer_id': customer_id, 
                                       'datetime': f'{curr_date} {hour}:{minute+extra_minute}:{second}'})
                id += 1

            for _ in range(num):
                sleep(0.05)
                extra_minute = random.randint(0, 4)
                second = random.randint(0, 59)
                customer_id_1 = random.randint(1, 1000)

                a = random.random()
                if a < 0.8:
                    producer.send('shop', {'id': id, 
                                       'type': 'Search', 
                                       'catagory_id':catagory_id,
                                       'customer_id': customer_id_1, 
                                       'datetime': f'{curr_date} {hour}:{minute+extra_minute}:{second}'})
                    
                b = random.random()
                if b < 0.7:
                    producer.send('shop', {'id': id, 
                                       'type': 'Buy', 
                                       'catagory_id':catagory_id, 
                                       'customer_id': customer_id_1, 
                                       'datetime': f'{curr_date} {hour}:{minute+extra_minute}:{second}'})
                    

                producer.send('shop', {'id': id, 
                                       'type': 'Click', 
                                       'catagory_id':catagory_id, 
                                       'customer_id': customer_id_1, 
                                       'datetime': f'{curr_date} {hour}:{minute+extra_minute}:{second}'})
                

            buy_rate = random.random()

            if buy_rate < 0.7:
                sleep(0.1)
                extra_minute = random.randint(0, 4)
                second = random.randint(0, 59)
                producer.send('shop', {'id': id, 
                                       'type': 'Buy', 
                                       'catagory_id':catagory_id, 
                                       'customer_id': customer_id, 
                                       'datetime': f'{curr_date} {hour}:{minute+extra_minute}:{second}'})
                id += 1

        minute += 5
        if minute == 60:
            hour +=1
            minute = 0
        
        if hour == 24:
            hour = 0
            date = date + timedelta(days= 1)


        
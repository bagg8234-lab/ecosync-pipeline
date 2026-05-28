import json
from kafka import KafkaConsumer
import time

def create_consumer(topic: str):
    """ Kafka Consumer 생성 """
    for i in range(5):  # 최대 5번 재시도
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers='kafka:9092',
                # 바이트 → JSON 문자열 → 딕셔너리 (Producer의 반대 과정)
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                # 컨슈머가 처음 시작할 때 토픽의 가장 오래된 메시지부터 읽도록 설정
                # earliest: 처음부터 읽기, latest: 새로 들어오는 메시지만 읽기
                auto_offset_reset='earliest',
                # 컨슈머 그룹을 설정하면 여러 컨슈머가 같은 그룹에 속해 있을 때 메시지를 분산 처리할 수 있음
                group_id='ecosync-group'  # 컨슈머 그룹 설정
            )
            print(f"Kafka Consumer 연결 성공! 토픽: {topic}")
            return consumer
        except Exception as e:
            print(f"Kafka Consumer 연결 실패 ({i+1}/5) — 5초 후 재시도... | 에러: {e}")
            time.sleep(5)
    raise Exception("Kafka Consumer 연결 실패 — 컨테이너 상태 확인 필요")

if __name__ == "__main__":
    print("Kafka Consumer 시작...")
    # 'generation' 토픽 구독
    consumer = create_consumer('generation')

    for message in consumer:
        data = message.value
        print(f"수신된 발전량 데이터: {data['city']} | {data['generation_kwh']} kWh | 시간: {data['timestamp']}")
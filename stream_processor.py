# stream_processor.py - 스트림 프로세서 (Consumer + Producer)

import json
from kafka import KafkaConsumer, KafkaProducer

# 토픽 이름 정의
input_topics = ['web-events', 'purchase-events']  # 읽을 토픽들
output_topic = 'analytics-events'  # 처리된 이벤트를 보낼 토픽
dlq_topic = 'analytics-dlq'  # 실패한 이벤트를 보낼 DLQ 토픽

print("=" * 60)
print("스트림 프로세서 시작")
print(f"입력: {input_topics}")
print(f"출력: {output_topic}")
print(f"DLQ: {dlq_topic}")
print("=" * 60)
print()

# Consumer 설정 - 여러 토픽을 동시에 구독
consumer = KafkaConsumer(
    *input_topics,  # web-events와 purchase-events 두 토픽 모두 구독
    bootstrap_servers='localhost:9092',
    group_id='stream-processor-group',
    auto_offset_reset='latest',
    enable_auto_commit=False,  # 수동 커밋으로 안전성 확보
    value_deserializer=lambda m: m.decode('utf-8')
)

# Producer 설정 - 처리된 데이터와 DLQ로 데이터를 보냄
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v.encode('utf-8')
)

def process_event(raw_message, source_topic):
    """
    이벤트를 공통 포맷으로 변환
    """
    # JSON 파싱 시도
    data = json.loads(raw_message)
    
    # web-events 처리
    if source_topic == 'web-events':
        # user_id가 필수 필드
        if 'user_id' not in data:
            raise ValueError("web-events에는 user_id가 필수입니다")
        
        return {
            'source': 'web',
            'user_id': data['user_id'],
            'event_type': data.get('event_type', 'unknown'),
            'timestamp': data.get('timestamp')
        }
    
    # purchase-events 처리
    elif source_topic == 'purchase-events':
        # user_id, product, amount가 필수 필드
        if 'user_id' not in data or 'product' not in data:
            raise ValueError("purchase-events에는 user_id와 product가 필수입니다")
        
        return {
            'source': 'purchase',
            'user_id': data['user_id'],
            'product': data['product'],
            'amount': data.get('amount', 0),
            'timestamp': data.get('timestamp')
        }
    
    else:
        raise ValueError(f"알 수 없는 토픽: {source_topic}")

try:
    for message in consumer:
        print("-" * 60)
        print(f"📨 수신: topic={message.topic}, partition={message.partition}, offset={message.offset}")
        print(f"   원본: {message.value[:100]}...")  # 처음 100자만 출력
        
        try:
            # 이벤트 처리
            processed = process_event(message.value, message.topic)
            
            # 정상 처리된 이벤트는 analytics-events로 전송
            producer.send(output_topic, value=processed)
            print(f"✅ 처리 성공 → {output_topic}")
            print(f"   변환된 데이터: {processed}")
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            # 처리 실패한 이벤트는 DLQ로 전송
            print(f"❌ 처리 실패: {e}")
            producer.send(dlq_topic, value=message.value)
        # 처리 완료 후 오프셋 커밋
        consumer.commit()
        print()

except KeyboardInterrupt:
    print("\n스트림 프로세서 종료.")
finally:
    consumer.close()
    producer.close()

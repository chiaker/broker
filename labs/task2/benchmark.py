import time
import json
import pika
import redis
import multiprocessing
import pandas as pd
import numpy as np
from tqdm import tqdm
import os

# Configuration
RABBITMQ_HOST = 'localhost'
REDIS_HOST = 'localhost'
QUEUE_NAME = 'bench_queue'

def get_payload(size_bytes):
    return "x" * size_bytes

def rabbit_producer(size, rate, duration, stop_event):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
        
        payload = get_payload(size)
        start_time = time.time()
        count = 0
        
        delay = 1.0 / rate if rate > 0 else 0
        
        while time.time() - start_time < duration and not stop_event.is_set():
            msg = {'ts': time.time(), 'payload': payload}
            channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(msg))
            count += 1
            if delay > 0:
                # Basic rate limiting
                elapsed = time.time() - start_time
                expected = count * delay
                if elapsed < expected:
                    time.sleep(expected - elapsed)
        
        connection.close()
        return count
    except Exception as e:
        print(f"Rabbit Producer Error: {e}")
        return 0

def rabbit_consumer(duration, results_dict, stop_event):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
        
        latencies = []
        
        def callback(ch, method, properties, body):
            msg = json.loads(body)
            latencies.append(time.time() - msg['ts'])
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
        
        start_time = time.time()
        # Consumer runs slightly longer than producer to catch remaining messages
        while time.time() - start_time < duration + 2 and not stop_event.is_set():
            connection.process_data_events(time_limit=0.5)
            
        connection.close()
        results_dict['count'] = len(latencies)
        results_dict['latencies'] = latencies
    except Exception as e:
        print(f"Rabbit Consumer Error: {e}")
        results_dict['count'] = 0
        results_dict['latencies'] = []

def redis_producer(size, rate, duration, stop_event):
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379)
        payload = get_payload(size)
        start_time = time.time()
        count = 0
        
        delay = 1.0 / rate if rate > 0 else 0
        
        while time.time() - start_time < duration and not stop_event.is_set():
            msg = {'ts': time.time(), 'payload': payload}
            r.lpush(QUEUE_NAME, json.dumps(msg))
            count += 1
            if delay > 0:
                elapsed = time.time() - start_time
                expected = count * delay
                if elapsed < expected:
                    time.sleep(expected - elapsed)
        return count
    except Exception as e:
        print(f"Redis Producer Error: {e}")
        return 0

def redis_consumer(duration, results_dict, stop_event):
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379)
        latencies = []
        
        start_time = time.time()
        while time.time() - start_time < duration + 2 and not stop_event.is_set():
            # Use blpop for blocking pop
            res = r.brpop(QUEUE_NAME, timeout=1)
            if res:
                msg = json.loads(res[1])
                latencies.append(time.time() - msg['ts'])
        
        results_dict['count'] = len(latencies)
        results_dict['latencies'] = latencies
    except Exception as e:
        print(f"Redis Consumer Error: {e}")
        results_dict['count'] = 0
        results_dict['latencies'] = []

def run_test(broker, size, rate, duration):
    print(f"Running test: Broker={broker}, Size={size}B, Rate={rate}msg/s, Duration={duration}s")
    
    # Clear queue before test
    if broker == 'rabbitmq':
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = conn.channel()
            channel.queue_delete(queue=QUEUE_NAME)
            channel.queue_declare(queue=QUEUE_NAME)
            conn.close()
        except: pass
        prod_func = rabbit_producer
        cons_func = rabbit_consumer
    else:
        try:
            r = redis.Redis(host=REDIS_HOST)
            r.delete(QUEUE_NAME)
        except: pass
        prod_func = redis_producer
        cons_func = redis_consumer

    manager = multiprocessing.Manager()
    results_dict = manager.dict()
    stop_event = multiprocessing.Event()
    
    consumer_proc = multiprocessing.Process(target=cons_func, args=(duration, results_dict, stop_event))
    consumer_proc.start()
    
    # Wait for consumer to be ready
    time.sleep(1)
    
    sent_count = prod_func(size, rate, duration, stop_event)
    
    consumer_proc.join()
    
    recv_count = results_dict.get('count', 0)
    latencies = results_dict.get('latencies', [])
    
    avg_latency = np.mean(latencies) * 1000 if latencies else 0
    p95_latency = np.percentile(latencies, 95) * 1000 if latencies else 0
    
    return {
        'broker': broker,
        'size_bytes': size,
        'target_rate': rate,
        'sent': sent_count,
        'received': recv_count,
        'loss_pct': ((sent_count - recv_count) / sent_count * 100) if sent_count > 0 else 0,
        'avg_latency_ms': avg_latency,
        'p95_latency_ms': p95_latency,
        'throughput': recv_count / duration if duration > 0 else 0
    }

if __name__ == '__main__':
    # Define scenarios
    sizes = [1280, 10240, 102400, 1024000] # 128B, 1KB, 10KB, 100KB
    rates = [10000, 50000, 100000]
    duration = 5 # Reduced for quicker initial data collection
    
    all_results = []
    
    print("Starting initial benchmarks...")
    for broker in ['redis', 'rabbitmq']:
        for size in sizes:
            for rate in rates:
                res = run_test(broker, size, rate, duration)
                all_results.append(res)
                print(f"Result: {res['sent']} sent, {res['received']} received, {res['avg_latency_ms']:.2f}ms avg")
                time.sleep(1)
    
    df = pd.DataFrame(all_results)
    df.to_csv('results2.csv', index=False)
    print("\nBenchmark results saved to results.csv")
    print(df.to_string())

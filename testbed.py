from threading import Thread, Event
from subprocess import Popen, STDOUT
from time import sleep, time

import random
import pika

class AppManagerThread(Thread):

	def __init__(self, min, max):
		Thread.__init__(self)
		self.min = min
		self.max = max
		self.stop_event = Event()
		self.processes = {}
		self.start_count = 1

	def run(self):
		current_count = 0
		while not self.stop_event.is_set():
			target_count = random.randint(self.min, self.max)
			print('Managing apps. Current count: %d, target count: %d' % (current_count, target_count))
			if target_count > current_count:
				self.start_processes(target_count - current_count)
			if target_count < current_count:
				self.stop_processes(current_count - target_count)
			current_count = target_count
			self.stop_event.wait(60)
		print('Stopping all apps. Current count: %d' % current_count)
		self.stop_processes(current_count)

	def signal_stop(self):
		self.stop_event.set()

	def start_processes(self, count):
		for i in range(count):
			print('Starting application instance %03d' % self.start_count)
			self.processes[self.start_count] = Popen(
				['python', 'app.py', '%03d' % self.start_count], 
				stderr=STDOUT,
				stdout=open('app_log_%03d' % self.start_count, 'w'))
			self.start_count += 1

	def stop_processes(self, count):
		for i in range(count):
			to_stop = self.processes.popitem()
			print('Stopping application instance %03d' % to_stop[0])
			to_stop[1].terminate()


class SenderThread(Thread):

	def __init__(self, conn_params, count, sent_times):
		Thread.__init__(self)
		self.conn_params = conn_params
		self.count = count
		self.sent_times = sent_times

	def run(self):
		conn = pika.BlockingConnection(self.conn_params)
		ch = conn.channel()
		key_sequences = {}
		key = 1
		for i in range(1, self.count+1):
			key = random.choice((key, random.randint(1, 101)))
			seq = key_sequences.get(key, 0) + 1
			key_sequences[key] = seq
			ch.basic_publish(
				exchange='e.order-test', 
				routing_key='rk_%03d' % key,
				body='msg_%05d_rk_%03d_seq_%03d' % (i, key, seq))
			sent_times[i] = time()
			if (i % 1000 == 0):
				print('Sent %d total messages' % i)
			sleep(0.005)
		ch.close()
		conn.close()		

class ReceiverThread(Thread):

	def __init__(self, conn_params, sent_times):
		Thread.__init__(self)
		self.conn_params = conn_params
		self.received = 0
		self.last_received = 0		
		self.sent_times = sent_times

	def run(self):
		self.out_csv = open('result_messages.csv', 'w')
		self.out_csv.write('sent,received,msg_number,routing_key,key_sequence,app,queue,app_time\n')
		self.conn = pika.BlockingConnection(self.conn_params)
		self.ch = self.conn.channel()
		self.ch.basic_consume(queue='q.order-test.results', auto_ack=True, on_message_callback=self.callback)
		self.conn.call_later(15, self.check_received)
		self.ch.start_consuming()
		self.ch.close()
		self.conn.close()
		self.out_csv.close()

	def callback(self, ch, method, properties, body):
		self.received += 1
		tokens = str(body).split('_')
		self.out_csv.write(
			'%f,%f,%s,%s,%s,%s,%s,%s\n'
			% (sent_times[int(tokens[1])], time(), tokens[1], tokens[3], tokens[5], tokens[7], tokens[9], tokens[11]))
	
	def check_received(self):
		if self.received == self.last_received:
			print('Did not receive messages in last 15 seconds. Stoppig result consumer.')
			self.ch.stop_consuming()
		else:
			print('Received %d total messages' % self.received)
			self.last_received = self.received
			self.conn.call_later(15, self.check_received)


def set_up_queues(conn_params, count):
	conn = pika.BlockingConnection(conn_params)
	ch = conn.channel()
	ch.exchange_delete(exchange='e.order-test')
	ch.exchange_declare(exchange='e.order-test', exchange_type='x-consistent-hash', durable=True)
	ch.queue_delete(queue='q.order-test.results')
	ch.queue_declare(queue='q.order-test.results', durable=True)
	for i in range(count):
		q_name = 'q.order-test.shard.%03d' % (i+1)
		ch.queue_delete(queue=q_name)
		ch.queue_declare(queue=q_name, durable=True)
		ch.queue_bind(exchange="e.order-test", queue=q_name, routing_key="1")
	ch.close()
	conn.close()


if __name__ == '__main__':
	conn_params = pika.ConnectionParameters(
		'localhost', 5672, '/',
		pika.PlainCredentials('guest', 'guest'))

	set_up_queues(conn_params, 10)
	
	apps = AppManagerThread(2, 6)
	apps.start()
	
	sent_times = {}
	sender = SenderThread(conn_params, 50000, sent_times)
	sender.start()
	receiver = ReceiverThread(conn_params, sent_times)
	receiver.start()

	sender.join()
	receiver.join()
	apps.signal_stop()
	apps.join()
	
	print('Done!')
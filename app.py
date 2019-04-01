from threading import Thread
from random import randint
from time import sleep, time

import sys
import pika

class ReceiverThread(Thread):

	def __init__(self, conn_params, queue_number, delay_range_ms):
		Thread.__init__(self)
		self.conn_params = conn_params
		self.received = 0
		self.last_received = 0
		self.delay_range_ms = delay_range_ms
		self.queue_number = queue_number

	def run(self):
		self.conn = pika.BlockingConnection(self.conn_params)
		while True:
			ch = self.conn.channel()
			if self.try_register_consumer(ch):
				print('Will consume from queue number %03d' % self.queue_number)
				ch.start_consuming()
			else:
				print('Queue number %03d is locked, will try later' % self.queue_number)
				sleep(10)
		
	def try_register_consumer(self, ch):
		queue_name='q.order-test.shard.%03d' % self.queue_number
		try:
			ch.basic_consume(
				queue=queue_name,
				consumer_tag='app_%s_cons_%03d' % (sys.argv[1], self.queue_number),
				exclusive=True,
				on_message_callback=self.on_message)
			return True
		except pika.exceptions.ChannelClosedByBroker as e:
			if e.reply_code == 403:
				return False
			else:
				raise

	def on_message(self, ch, method, properties, body):
		self.received += 1
		ch.basic_publish(
			exchange='', routing_key='q.order-test.results', 
			body='%s_app_%s_q_%03d_apptime_%f' % (body, sys.argv[1], self.queue_number, time()))
		delay_ms = randint(self.delay_range_ms[0], self.delay_range_ms[1])
		print('Received: %s, total count: %d, will process for %d ms' % (str(body), self.received, delay_ms))
		sleep(float(delay_ms) / 1000)
		ch.basic_ack(method.delivery_tag)


if __name__ == '__main__':
	conn_params = pika.ConnectionParameters(
		'localhost', 5672, '/',
		pika.PlainCredentials('guest', 'guest'))	
	
	delay_min = randint(20, 300)
	delay_max = delay_min + randint(10, 100)
	print('Randomized delay range: %d to %d ms' % (delay_min, delay_max))
	
	receivers = []
	for i in range(1, 11):
		receiver = ReceiverThread(conn_params, i, (delay_min, delay_max))
		receiver.start()
		receivers.append(receiver)
	for receiver in receivers:
		receiver.join()
	
	print('Done!')
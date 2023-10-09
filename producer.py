import sys
import socket


def req_zookeeper(producer,ip_addr):
	PORT = 2181
	producer.connect((ip_addr, PORT))
	producer.send("producer".encode('utf-8'))
	MASTER_PORT = producer.recv(1024).decode('utf-8')
	producer.close()
	return MASTER_PORT

def req_broker(producer,ip_addr,MASTER_PORT):
	PORT = int(MASTER_PORT)
	producer.connect((ip_addr,PORT))
	producer.send("producers".encode('utf-8'))
	topic_name = input("Enter the topic name : ")
	message = input("Enter the message : ")
	producer.send(('{"topic":"'+topic_name+'","message":"'+ message +'"}').encode('utf-8'))
	producer.close()

if __name__ == '__main__':
	try:
		producer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		hostname = socket.gethostname()
		ip_addr = socket.gethostbyname(hostname)
		MASTER_PORT = req_zookeeper(producer,ip_addr)
	except socket.error as err:
		print(err)
		
	try:
		producer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		req_broker(producer,ip_addr,MASTER_PORT)
	except socket.error as err:
		print("No master available!!")
import traceback

import rpyc
import hashlib
import os
import sys
from metastore import ErrorResponse

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""

class SurfStoreClient():

	"""
	Initialize the client and set up connections to the block stores and
	metadata store using the config file
	"""
	def __init__(self, config):
		self.config = config
		self.block_num, self.header, self.host, self.port = self.parse_config()
		# crate a metadata channel
		self.meta_channel = rpyc.connect(self.host[0], self.port[0])
		# create a metadata stub, in this project we assume one metadata store
		self.meta_stub = self.meta_channel.root

		# create multiple block store channels
		self.block_channel = [None]*self.block_num
		self.block_stub = [None]*self.block_num
		for i in range(0, self.block_num):
			# since the element 0 of port and host is for metadata, we start with 0 + 1 for block store stubs
			self.block_channel[i] = rpyc.connect(self.host[i+1], self.port[i+1])
			self.block_stub[i] = self.block_channel[i].root

	def findServer(self, hash_key):
		return int(hash_key, 16) % self.block_num


	def splitFile(self, real_path):
		file = open(real_path, 'rb')
		key_block_table = {}
		key_server_table = {}
		hash_list = []
		block_size = 4096
		block = file.read(block_size)
		while block:
			hashed_key = hashlib.sha256(block).hexdigest()
			key_block_table[hashed_key] = block
			key_server_table[hashed_key] = self.findServer(hashed_key)
			hash_list.append(hashed_key)
			block = file.read(block_size)
		file.close()
		return key_block_table, key_server_table, hash_list
	"""
	upload(filepath) : Reads the local file, creates a set of 
	hashed blocks and uploads them onto the MetadataStore 
	(and potentially the BlockStore if they were not already present there).
	"""
	def upload(self, filepath):
		# real_path = os.path.realpath(filepath)
		real_path = filepath
		# get the file name
		if real_path.find('\\') == -1 and real_path.find('/') == -1:
			# if no '/' present in the path, the file name is just the real_path
			file_name = real_path
			# print('no slash', real_path)
		else:
			# else take the last term of the real_path seperated by /
			if real_path.find('\\') != -1:
				file_name = real_path.split('\\')[-1]
			else:
				file_name = real_path.split('/')[-1]
			# print(file_name)
		# make a rpc call to the metadata.root.read_file
		v, _ = self.meta_stub.read_file(file_name)

		# split the file, get the mapping of key, block and destination (the index of block store)
		key_block_table, key_server_table, hash_list = self.splitFile(real_path)

		while True:
			# update version number
			v += 1
			try:
				# get the hash list of the missing block
				response = self.meta_stub.modify_file(file_name, v, hash_list)
				if response == 'OK':
					print('OK')
					return
			except rpyc.core.vinegar.GenericException as e:
				# print(e.error)
				if e.error_type == 1:
					miss_block_list = list(eval(e.missing_blocks))
					for key in miss_block_list:
						destination = key_server_table[key]
						block_sent = key_block_table[key]
						self.block_stub[destination].store_block(key, block_sent)
				# elif e.error_type == 2:
				# 	print('error type 2: wrong version')
				# 	return
				# elif e.error_type == 3:
				# 	print('error type 3: file not found')
				# 	return
			except Exception:
				return

		# # if response is 'OK'
		# if response == 'OK':
		# 	print('upload succeed')
		# # for each hash value in the missing block list, send the file to the destination
		# if response != 'OK':
		# 	error_type = response.error_type
		#
		# 	if error_type == 1:
		# 		miss_block_list = response.missing_blocks
		# 		for key in miss_block_list:
		# 			destination = key_server_table[key]
		# 			block_sent = key_block_table[key]
		# 			self.block_stub[destination].store_block(key, block_sent)
		# 	elif error_type == 2:
		# 		print('error type 2: wrong version')
		# 		return
		#
		# 	elif error_type == 3:
		# 		print('error type 3: file not found')
		# 		return
		# response = self.meta_stub.modify_file(file_name, new_v, hash_list)
		# if response == 'OK':
		# 	print('OK')
		# else:
		# 	print('unexpected Error in uploading file')
		# return

	"""
	delete(filename) : Signals the MetadataStore to delete a file.
	"""
	def delete(self, filename):
		v, hl = self.meta_stub.read_file(filename)
		if hl:
			hl = list(hl)
		if v == 0:
			print('Not Found')
			return
		if not hl:
			print('Not Found')
			return
		while True:
			v += 1
			try:
				self.meta_stub.delete_file(filename, v)
				print('OK')
				return
			except rpyc.core.vinegar.GenericException as e:
				pass

	"""
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
	"""
	def download(self, filename, location):
		v, hl = self.meta_stub.read_file(filename)
		# print(v, hl)
		real_file_path = os.path.realpath(location + '/' + filename)
		# real_file_path = location + '/' + filename
		if not hl:
			print('Not Found')
			return
		hl = list(hl)

		if os.path.isfile(real_file_path):
			# see if file already exist, if exist, download the necessary ones
			key_block_table, key_server_table, hash_list = self.splitFile(real_file_path)
			block_set = []
			for key in hl:
				if key in key_block_table:
					block_set.append(key_block_table[key])
					continue
				source = self.findServer(key)
				block_received = self.block_stub[source].get_block(key)
				block_set.append(block_received)
			file = open(real_file_path, 'wb')
			for b in block_set:
				file.write(b)
			file.close()
			print('OK')
			return
		else:
			# if the file not exist, directly download everything
			file = open(real_file_path, 'wb')
			for key in hl:
				source = self.findServer(key)
				block_received = self.block_stub[source].get_block(key)
				file.write(block_received)
			file.close()
			print('OK')
			return

	"""
	 Use eprint to print debug messages to stderr
	 E.g - 
	 self.eprint("This is a debug message")
	"""
	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)

	def parse_config(self):
		real_path = os.path.realpath(self.config)
		file = open(real_path, 'rb')
		header_field = []
		host_field = []
		port_field = []
		config_string = file.readline()
		config_string = config_string.decode('utf-8')
		idx = config_string.find(': ')
		num_block = int(config_string[idx + 2])
		config_string = file.readline()
		while config_string:
			config_string = config_string.decode('utf-8')
			idx = config_string.find(': ')
			header_field.append(config_string[0:idx])
			idx2 = config_string.find(':', idx + 2)
			host_field.append(config_string[idx + 2:idx2])
			idx_next_line = config_string.find('\r\n', idx2)
			if idx_next_line != -1:
				port_field.append(int(config_string[idx2 + 1:idx_next_line]))
			else:
				port_field.append(int(config_string[idx2 + 1:]))
			config_string = file.readline()
		file.close()
		return num_block, header_field, host_field, port_field


if __name__ == '__main__':

	client = SurfStoreClient(sys.argv[1])
	operation = sys.argv[2]
	if operation == 'upload':
		client.upload(sys.argv[3])
	elif operation == 'download':
		client.download(sys.argv[3], sys.argv[4])
	elif operation == 'delete':
		client.delete(sys.argv[3])
	else:
		print("Invalid operation")

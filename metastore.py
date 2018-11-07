import rpyc
import sys
import os
import threading

'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
class ErrorResponse(Exception):
    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        self.error = message

    def missing_blocks(self, hashlist):
        self.error_type = 1
        self.missing_blocks = hashlist

    def wrong_version_error(self, version):
        self.error_type = 2
        self.current_version = version

    def file_not_found(self):
        self.error_type = 3


# class ErrorResponse:
#     def __init__(self, message):
#         self.error = message
#         self.error_type = None
#         self.missing_blocks = None
#         self.current_version = None
#         self.hashlist = None
#
#     def missing_blocks(self, hashlist):
#         self.error_type = 1
#         self.missing_blocks = hashlist
#
#     def wrong_version_error(self, version):
#         self.error_type = 2
#         self.current_version = version
#
#     def file_not_found(self):
#         self.error_type = 3

'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):


    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """
    def __init__(self, config):
        self.config_file = config
        self.lock = threading.Lock()
        num_block, header_field, host_field, port_field = self.parse_config()
        self.num_of_blockstores = num_block
        self.block_connections = []
        for i in range(1, len(header_field)):
            conn = rpyc.connect(host_field[i], port_field[i])
            self.block_connections.append(conn)

        # version_map key: filename, val: version
        self.version_map = {}
        # hashlist_map key: filename, val: list of block sha256 values
        self.hashlist_map = {}

    def parse_config(self):
        real_path = os.path.realpath(self.config_file)
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
    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_modify_file(self, filename, version, hashlist):
        hashlist = list(hashlist)
        self.lock.acquire()
        try:
            server_version = 0
            if filename in self.version_map:
                server_version = self.version_map[filename]
            # if version != server_version + 1, send wrong_version_error response
            if version != server_version + 1:
                error_response = ErrorResponse('Wrong Version Error')
                error_response.wrong_version_error(server_version)
                raise error_response

            # get missing blocks
            missing_blocks = []
            for hash in hashlist:
                server_idx = self.find_server(hash)
                if not self.block_connections[server_idx].root.has_block(hash):
                    missing_blocks.append(hash)

            # if missing blocks exist, notify client to store missing blocks, and update metadata store
            # as well
            if len(missing_blocks) > 0:
                error_response = ErrorResponse('Missing Blocks')
                error_response.missing_blocks(missing_blocks)
                self.version_map[filename] = version
                self.hashlist_map[filename] = hashlist
                raise error_response

            # if no missing blocks, update metadata store and return 'OK'
            self.version_map[filename] = version
            self.hashlist_map[filename] = hashlist
            return 'OK'
        finally:
            self.lock.release()

    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_delete_file(self, filename, version):
        server_version = 0
        self.lock.acquire()
        try:
            if filename in self.version_map:
                server_version = self.version_map[filename]

            if server_version == 0 or version != server_version + 1:
                error_response = ErrorResponse('Wrong Version Error')
                error_response.wrong_version_error(server_version)
                raise error_response

            self.version_map[filename] = version
            self.hashlist_map[filename] = None
            return 'OK'
        finally:
            self.lock.release()

    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_read_file(self, filename):
        version = 0
        print('version', self.version_map.keys())
        print('hash list', self.hashlist_map.keys())
        if filename in self.version_map:
            version = self.version_map.get(filename)
        hashlist = None
        if filename in self.hashlist_map:
            hashlist = self.hashlist_map.get(filename)
        return version, hashlist


    def find_server(self, h):
        return int(h, 16) % self.num_of_blockstores


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
    server.start()


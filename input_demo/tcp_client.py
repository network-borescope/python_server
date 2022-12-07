import socket
import sys

if len(sys.argv) != 3 and len(sys.argv) != 4:
    print("Usage: python3 client_tcp.py <tc_result.json> <model>")
    sys.exit(1)

filename = sys.argv[1]
model = sys.argv[2]
model_file = sys.argv[3] if len(sys.argv) == 4 else None

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 8080        # The port used by the server
BUFFER_SIZE = 4096

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s, open(filename, "r") as f:
    s.connect((HOST, PORT))
    
    # sending
    data = ""
    for line in f: data += line

    header = None
    if model_file:
        header = "# 1.0 {} {}\r\n".format(model, model_file)
    else:
        header = "# 1.0 {}\r\n".format(model)

    # expected
    data = bytes(header, 'utf-8') + bytes(data, 'utf-8') + b"\r\n\r\n"
    
    # wrong header
    #data = bytes(header, 'utf-8') + bytes(data, 'utf-8') + b"\r\n\r\n"
    
    # missing header
    #data = bytes(data, 'utf-8') + b"\r\n\r\n"

    # missing end
    #data = bytes(header, 'utf-8') + bytes(data, 'utf-8')
    s.sendall(data)
    

    # receiving
    data = ""
    while True:
        buffer = s.recv(BUFFER_SIZE)
        if not buffer:
            break
    
        data += buffer.decode("utf-8")
    
    print(data)

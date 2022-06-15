# Python Post Processing Server
Python Server capable of IA and Statistical processing.

## Executing the Server
``` Bash
cd server
bash run_server.sh
```

## Testing using input demo
After executing the server, its possible to use the tcp_client.py program to communicate with the python server. The tcp client program used for testing has two parameters:
1) an input file (sample data)
2) a choosen processing to be made by the python server.

This test can be executed as the examples bellow:

### Example 1: Calculate CDF of input sample "pop_x_service.txt"
Navigate to the `input_demo` directory and do:
``` Bash
python3 tcp_client.py pop_x_service.txt /stat/cdf
```

### Example 2: Calculate CCDF of input sample "pop_x_service.txt"
Navigate to the `input_demo` directory and do:
``` Bash
python3 tcp_client.py pop_x_service.txt /stat/ccdf
```


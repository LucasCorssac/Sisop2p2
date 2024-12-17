all: client server

client:  
	gcc client.c -o client
server:  
	gcc server.c -o server

debug: server_debug client_debug

client_debug:  
	gcc client.c -o client -DDEBUG -ggdb
server_debug:  
	gcc server.c -o server -DDEBUG -ggdb

clean:
	rm client server

wclean:
	rm client.exe server.exe

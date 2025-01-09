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

# Define variables
CLIENT_IMAGE_NAME := my_client_image
SERVER_IMAGE_NAME := my_server_image
CLIENT_CONTAINER_NAME := client_container_$(shell date +%s)
SERVER_CONTAINER_NAME := server_container_$(shell date +%s)
# INSTANCE_COUNT := $(or $(INSTANCES), 1)

# Build the Docker image
docker_build: docker_client docker_server

docker_client:
	docker build -t $(CLIENT_IMAGE_NAME) -f Dockerfile.client .
docker_server:
	docker build -t $(SERVER_IMAGE_NAME) -f Dockerfile.server .


# Run the Docker container with a unique name
run_client: docker_client
	docker run --rm -it --name $(CLIENT_CONTAINER_NAME) $(CLIENT_IMAGE_NAME) $(ARGS)
run_server: docker_server
	docker run -it --name $(SERVER_CONTAINER_NAME) $(SERVER_IMAGE_NAME) $(ARGS)

run_client_rands2: docker_client
	docker run --rm -i --name $(CLIENT_CONTAINER_NAME) $(CLIENT_IMAGE_NAME) $(ARGS) < rand2

run_client_rands3: docker_client
	docker run --rm -i --name $(CLIENT_CONTAINER_NAME) $(CLIENT_IMAGE_NAME) $(ARGS) < rand3

run_client_rands4: docker_client
	docker run --rm -i --name $(CLIENT_CONTAINER_NAME) $(CLIENT_IMAGE_NAME) $(ARGS) < rand4

# docker_test_servers: docker_server
# 	@for i in $(shell seq 1 $(INSTANCE_COUNT)); do \
#         docker run --rm -it --name server_container_$$i $(SERVER_IMAGE_NAME) $(ARGS); \
#     done 

# Clean up (optional)
docker_clean:
	docker rmi $(IMAGE_NAME)
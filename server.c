#ifdef DEBUG
    #define debug_print(...) printf(__VA_ARGS__)
#else
    #define debug_print(...) do{ } while ( 0 )
#endif

#define handle_error_en(en, msg) \
  do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

#define handle_error(msg) \
  do { perror(msg); exit(EXIT_FAILURE); } while (0)

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#include "packet.h"

#define MAX_CLIENTS 10
#define MAX_REPLICAS 5

void * process_request(void *arg);

enum SERVER_STATE {
    MASTER,
	REPLICA,
	WAITING,
	ELECTION
};


typedef struct REPLICA_TABLE_CELL
{
	int empty;
	
	#ifdef DEBUG
    	int id;
	#endif
	
    struct sockaddr_in cli_addr;

	long long last_seqn;
    long long last_num_reqs;  
    long long last_total_sum;
	long long last_value; 

	packet pckt_cli;

	pthread_mutex_t cli_lock;
} replica_table_cell;

typedef struct CLIENT_TABLE_CELL
{
	int empty;
	
	#ifdef DEBUG
    	int id;
	#endif
	
    struct sockaddr_in cli_addr;

	long long last_seqn;
    long long last_num_reqs;  
    long long last_total_sum;
	long long last_value; 

	packet pckt_cli;

	pthread_mutex_t cli_lock;
} client_table_cell;

struct SHARED_VALUES
{
	long long total_sum;
	long long total_reqs;
};

struct SHARED_VALUES shared_values;

client_table_cell client_table[MAX_CLIENTS];

pthread_mutex_t shared_lock;

void print_timestamp()
{
	time_t t = time(NULL);
 	struct tm tm = *localtime(&t);
  	printf("%d-%02d-%02d %02d:%02d:%02d ",
	 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}

int sockfd;

int main(int argc, char *argv[])
{
	debug_print("Debug flag was defined\n");

	enum SERVER_STATE server_state = WAITING;

	int n;
	socklen_t clilen = sizeof(struct sockaddr_in);
	struct sockaddr_in brdcst_addr, serv_addr, cli_addr;
	int serv_port;
	
	packet pckt_cli, pckt_ack_disc, pckt_ack_req;
	
	// GET PORT NUMBER
	if (argc < 2) {
		fprintf(stderr, "usage %s <port number>\n", argv[0]);
		exit(1);
	}
	sscanf(argv[1], "%d", &serv_port);
	debug_print("port given by user: %d\n", serv_port);


	// INITIALIZE MUTEX    
    n = pthread_mutex_init(&shared_lock, NULL);
    if (n != 0)
        handle_error_en(n, "pthread_mutex_init error");
    

	// INITIALIZE CLIENT TABLE
	for(int i = 0; i < MAX_CLIENTS; i++)
	{
		client_table[i].empty = 1;
		client_table[i].last_seqn = 0;
		n = pthread_mutex_init (&client_table[i].cli_lock, NULL);
		if (n != 0)
        	handle_error_en(n, "pthread_mutex_init error");
	}

	// INITIALIZE SHARED VALUES
	shared_values.total_reqs = 0;
	shared_values.total_sum = 0;

	// CREATE SOCKET	
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1)
		handle_error("ERROR opening socket");

	// GIVE BROADCAST PERMISSION	
	int broadcastPermission;
	broadcastPermission = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_BROADCAST, (void *) &broadcastPermission, sizeof(broadcastPermission)) < 0)
		handle_error("setsockopt error");	

	// BIND SOCKET
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(serv_port);
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	bzero(&(serv_addr.sin_zero), 8);    
	 
	if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
		handle_error("ERROR on binding");


	//////////////////// SEARCH FOR OTHER SERVERS

	// SET BROADCAST ADDRESS
	memset(&brdcst_addr, 0, sizeof(brdcst_addr));
	brdcst_addr.sin_family = AF_INET;     
	brdcst_addr.sin_port = htons(cli_port);
	brdcst_addr.sin_addr.s_addr = inet_addr("255.255.255.255");	 // broadcast IP
	bzero(&(brdcst_addr.sin_zero), 8);

	// SET SOCKET REQUEST TIMEOUT PARAMETERS
	struct timeval timeout = {.tv_sec = 0, .tv_usec = 10000}; // 10 ms

	if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
		perror("setsockopt failed");
	}	


	// PRINT INITIALIZATION MESSSAGE
	print_timestamp(); printf("num_reqs %lld total_sum %lld\n",shared_values.total_reqs, shared_values.total_sum);
		
	while (1) 
	{
		// WAIT FOR PACKETS
		n = recvfrom(sockfd, &pckt_cli, sizeof(packet), 0, (struct sockaddr *) &cli_addr, &clilen);
		if (n < 0)
		{
			switch (errno)
			{
			case EBADF:
				printf("ERROR EBADF\n");
				break;
			case ECONNREFUSED:
				printf("ERROR ECONNREFUSED\n");
				break;
			case EFAULT:
				printf("ERROR EFAULT\n");
				break;
			case EINTR:
				printf("ERROR EINTR\n");
				break;
			case EINVAL:
				printf("ERROR EINVAL\n");
				break;
			case ENOMEM:
				printf("ERROR ENOMEM\n");
				break;
			case ENOTCONN:
				printf("ERROR ENOTCONN\n");
				break;
			case ENOTSOCK:
				printf("ERROR ENOTSOCK\n");
				break;	
			default:
				printf("ERROR NOT SPECIFIED recvfrom\n");
				break;
			}
			handle_error("ERROR on recvfrom\n");
		}
		
		// PROCESS PACKET
		switch (pckt_cli.type)
		{
			case DISC:
				debug_print("Found a discovery!\n");
				// ADD CLIENT TO CLIENT TABLE
				int client_already_exists = -1;
				int empty_slot = -1;
				for(int i = 0; i < MAX_CLIENTS; i++)
				{
					#ifdef DEBUG
						if(!client_table[i].empty && client_table[i].id == pckt_cli.id)
							client_already_exists = i;
					#else
						if(!client_table[i].empty &&
							client_table[i].cli_addr.sin_addr.s_addr == cli_addr.sin_addr.s_addr)
							client_already_exists = i;
					#endif
					if(empty_slot == -1 && client_table[i].empty)
						empty_slot = i;
				}
				if (client_already_exists == -1 && empty_slot != -1)
				{
					debug_print("sending DISC ACK\n");
					#ifdef DEBUG
						client_table[empty_slot].id = pckt_cli.id;
					#endif	
					client_table[empty_slot].empty = 0;
					client_table[empty_slot].cli_addr = cli_addr;

					// SEND ACK
					pckt_ack_disc.type = DISC_ACK;
					n = sendto(sockfd, &pckt_ack_disc, sizeof(packet), 0,(struct sockaddr *) &cli_addr, sizeof(cli_addr));
					if (n  < 0)
						handle_error("ERROR on sendto");					
				}
				break;
			case REQ:
				// FIND CLIENT IN TABLE
				debug_print("Found a REQ\n");
				int cli_cell = -1, i = 0;
				do
				{
					#ifdef DEBUG
						if (client_table[i].id == pckt_cli.id)
						{
							cli_cell = i;
							client_table[cli_cell].pckt_cli = pckt_cli;
						}
					#else
						if (client_table[i].cli_addr.sin_addr.s_addr == cli_addr.sin_addr.s_addr)
						{
							cli_cell = i;
							client_table[cli_cell].pckt_cli = pckt_cli;
						}
					#endif
					i++;
				} while (cli_cell == -1 && i < MAX_CLIENTS);
				
				// CREATE THREAD TO HANDLE REQUEST
				if (cli_cell != -1)
				{
					pthread_t id;
					int* cli_cell_ptr = malloc(sizeof(int));
					*cli_cell_ptr = cli_cell;
					n = pthread_create(&id, NULL, &process_request, cli_cell_ptr);
					if (n != 0)
						handle_error("Error creating thread\n");
				}
			break;
		} 		
	}

	// DESTROY MUTEXES 
	pthread_mutex_destroy(&shared_lock);
	for(int i = 0; i < MAX_CLIENTS; i++)
	{
		pthread_mutex_destroy(&client_table[i].cli_lock);
	}
	
	close(sockfd);
	return 0;
}

void* process_request(void *arg)
{
	pthread_detach(pthread_self());		

	int cli_index = *(int*)arg;
	free(arg);
	
	pthread_mutex_lock(&client_table[cli_index].cli_lock);

	packet pckt_ack_req, pckt_cli;
	pckt_cli = client_table[cli_index].pckt_cli;

	struct sockaddr_in cli_addr =  client_table[cli_index].cli_addr;
	
	if (client_table[cli_index].last_seqn ==
		pckt_cli.req.seqn -1)
	{
		// UPDATE SHARED VALUES
		pthread_mutex_lock(&shared_lock);
			shared_values.total_reqs++;
			shared_values.total_sum  += pckt_cli.req.value;
			client_table[cli_index].last_num_reqs = shared_values.total_reqs;
			client_table[cli_index].last_total_sum = shared_values.total_sum;
		pthread_mutex_unlock(&shared_lock);

		// UPDATE CLIENT TABLE
		client_table[cli_index].last_seqn++;
		client_table[cli_index].last_value = pckt_cli.req.value;
		
		print_timestamp(); printf("client %s id_req %lld value %lld num_reqs %lld total_sum %lld\n",
								inet_ntoa(cli_addr.sin_addr),
								pckt_cli.req.seqn,
								pckt_cli.req.value,
								client_table[cli_index].last_num_reqs,
								client_table[cli_index].last_total_sum
								);
	}
	else
	{
		print_timestamp(); printf("client %s DUP!! id_req %lld value %lld num_reqs %lld total_sum %lld\n",
	 						   inet_ntoa(cli_addr.sin_addr),
							   pckt_cli.req.seqn,
							   pckt_cli.req.value,
							   client_table[cli_index].last_num_reqs,
							   client_table[cli_index].last_total_sum
							   );
	}

		pckt_ack_req.type = REQ_ACK;
		pckt_ack_req.ack.value = client_table[cli_index].last_value;
		pckt_ack_req.ack.seqn =  client_table[cli_index].last_seqn;
		pckt_ack_req.ack.num_reqs = client_table[cli_index].last_num_reqs;
		pckt_ack_req.ack.total_sum = client_table[cli_index].last_total_sum;

		// SEND ACK
		sendto(sockfd, &pckt_ack_req, sizeof(packet), 0,(struct sockaddr *) &cli_addr, sizeof(cli_addr));

		pthread_mutex_unlock(&client_table[cli_index].cli_lock);
}

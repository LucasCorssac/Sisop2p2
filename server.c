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
#include <ifaddrs.h>

#include "packet.h"

#define MAX_CLIENTS 10
#define MAX_REPLICAS 4

void * process_request(void *arg);

enum SERVER_STATE {
    LEADER,
	REPLICA,
	SYNCING,
	ELECTING
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

struct SERVER_CELL
{
	struct sockaddr_in serv_addr;
	int found_all;	
};

void print_timestamp()
{
	time_t t = time(NULL);
 	struct tm tm = *localtime(&t);
  	printf("%d-%02d-%02d %02d:%02d:%02d ",
	 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}

int sockfd;

// Function to get the server's IP address
void get_server_ip(char *ip_buffer, size_t buffer_size) {
    struct ifaddrs *ifaddr, *ifa;
    int family;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        exit(EXIT_FAILURE);
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        family = ifa->ifa_addr->sa_family;

        if (family == AF_INET) {
            if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                            ip_buffer, buffer_size, NULL, 0, NI_NUMERICHOST) == 0) {
                // Print the IP address of the interface
                printf("Interface: %s\tAddress: %s\n", ifa->ifa_name, ip_buffer);
            }
        }
    }

    freeifaddrs(ifaddr);
}

void get_interface_ip(const char *interface, char *ip_buffer, size_t buffer_size) {
    struct ifaddrs *ifaddr, *ifa;
    int family;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        exit(EXIT_FAILURE);
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        family = ifa->ifa_addr->sa_family;

        if (family == AF_INET && strcmp(ifa->ifa_name, interface) == 0) {
            if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                            ip_buffer, buffer_size, NULL, 0, NI_NUMERICHOST) == 0) {
                break;
            }
        }
    }

    freeifaddrs(ifaddr);
}

int main(int argc, char *argv[])
{
	debug_print("Debug flag was defined\n");

	enum SERVER_STATE server_state = SYNCING;

	int n;
	socklen_t clilen = sizeof(struct sockaddr_in);
	struct sockaddr_in brdcst_addr, my_addr, cli_addr, serv_addr;
	int my_port;
	
	packet pckt_cli, pckt_ack_disc, pckt_ack_req;
	
	// GET PORT NUMBER
	if (argc < 2) {
		fprintf(stderr, "usage %s <port number>\n", argv[0]);
		exit(1);
	}
	sscanf(argv[1], "%d", &my_port);
	debug_print("port given by user: %d\n", my_port);
	int num_replicas = MAX_REPLICAS;
	if (argc == 3)
	{
		sscanf(argv[2], "%d", &num_replicas);
	}
	int num_servers = num_replicas + 1;


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

	int reuseAddr = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuseAddr, sizeof(reuseAddr));
	

	// // BIND SOCKET
	memset(&my_addr, 0, sizeof(my_addr));
	my_addr.sin_family = AF_INET;
	my_addr.sin_port = htons(my_port);
	my_addr.sin_addr.s_addr = INADDR_ANY;  

	if (bind(sockfd, (struct sockaddr *) &my_addr, sizeof(my_addr)) < 0) 
		handle_error("ERROR on binding");

	//////////////////// SEARCH FOR OTHER SERVERS

	char *broadcastIP;
	broadcastIP = "255.255.255.255";

	// SET BROADCAST ADDRESS
	memset(&brdcst_addr, 0, sizeof(brdcst_addr));
	brdcst_addr.sin_family = AF_INET;     
	brdcst_addr.sin_port =  htons(my_port);
	brdcst_addr.sin_addr.s_addr = inet_addr(broadcastIP);	 // broadcast IP

	// SET SOCKET REQUEST TIMEOUT PARAMETERS
	struct timeval timeout = {.tv_sec = 0, .tv_usec = 10000}; // 10 ms

	if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
		perror("setsockopt failed");
	}

	socklen_t serv_addr_len = sizeof(serv_addr);	

	packet pckt_serv_rply, pckt_serv_disc;
	memset(&pckt_serv_rply, 0, sizeof(packet));

	pckt_serv_disc.type = SERV_DISC;

	struct SERVER_CELL server_list[num_servers];
	memset(server_list, 0, sizeof(server_list));


	// GET SERVER'S OWN IP
	char ip_buffer[NI_MAXHOST];

	// // Get the server's IP address
    // get_server_ip(ip_buffer, sizeof(ip_buffer));
    // printf("Server IP: %s\n", ip_buffer);

    // Get the IP address of the eth0 interface
    get_interface_ip("eth0", ip_buffer, sizeof(ip_buffer));
    printf("IP address of eth0: %s\n", ip_buffer);

	int found_servers = 0;
	int found_alls = 0;
	int server_already_found = 0;
	printf("searching for servers!\n");
	do
	{
		if (found_servers >= num_servers)
			pckt_serv_disc.type = SERV_FOUND_ALL;
		
		//printf("sending message\n");
		n = sendto(sockfd, &pckt_serv_disc, sizeof(packet), 0, (struct sockaddr *) &brdcst_addr, sizeof(brdcst_addr));
		if (n < 0)
			handle_error("ERROR sendto\n");

		// WAIT FOR OTHER SERVERS
		//printf("waiting reply\n");
		n = recvfrom(sockfd, &pckt_serv_rply, sizeof(packet), 0, (struct sockaddr *) &serv_addr, &serv_addr_len);
		if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
			handle_error("ERROR recvfrom\n");
		
		// CHECK IF SERVER SENT CORRECT RESPONSE
		if (pckt_serv_rply.type == SERV_DISC && found_servers < num_servers) // strcmp(ip_buffer, inet_ntoa(serv_addr.sin_addr)) != 0
		{
			//printf("Got reply, checking if severs been registerd already\n");
			server_already_found = 0;
			for (int i = 0; i < found_servers; i++)
			{
				//if (strcmp(inet_ntoa(server_list[i].serv_addr.sin_addr), inet_ntoa(serv_addr.sin_addr)) == 0)
				if (server_list[i].serv_addr.sin_addr.s_addr == serv_addr.sin_addr.s_addr)
				{
					//printf("server has been found before\n");
					server_already_found = 1;
					break;
				}
			}
			if (!server_already_found)
			{
				print_timestamp(); printf("FOUND SERVER AT: %s\n", inet_ntoa(serv_addr.sin_addr));
				server_list[found_servers].serv_addr = serv_addr;
				found_servers += 1;
			}			
		}
		else if (pckt_serv_rply.type == SERV_FOUND_ALL)
		{
			for (int i = 0; i < found_servers; i++)
			{
				//if (strcmp(inet_ntoa(server_list[i].serv_addr.sin_addr), inet_ntoa(serv_addr.sin_addr)) == 0)
				if (server_list[i].serv_addr.sin_addr.s_addr == serv_addr.sin_addr.s_addr)
				{
					if(!server_list[i].found_all)
					{
						server_list[i].found_all = 1;
						print_timestamp(); printf("GOT FOUND ALL FROM: %s\n", inet_ntoa(serv_addr.sin_addr));
						found_alls++;
					}
				}
			}
		}
		else if (pckt_serv_rply.type == I_AM_LEADER)
		{
			goto am_leader_late;
		}
		else if (pckt_serv_rply.type == YOU_ARE_LEADER)
		{
			goto you_are_leader_late;
		}
	} while (found_alls < num_servers);

	printf("ALL SERVERS FOUND \n");

	// GET LEADER BASED ON IP
	int leader_idx = 0;
	struct sockaddr_in leader_addr;
	for (int i = 0; i < num_servers; i++)
	{
		if (server_list[i].serv_addr.sin_addr.s_addr > server_list[leader_idx].serv_addr.sin_addr.s_addr)
		{
			leader_idx = i;
			leader_addr = server_list[i].serv_addr;
		}
	}
	printf("Leader is: %s\n", inet_ntoa(server_list[leader_idx].serv_addr.sin_addr));

	// SEND YOU ARE LEADER MESSAGE
	packet pckt_post_sync, pckt_psync_rply;
	memset(&pckt_psync_rply, 0, sizeof(packet));

	pckt_post_sync.type = YOU_ARE_LEADER;
	do
	{
		printf("Sending you are leader message!\n");
		n = sendto(sockfd, &pckt_post_sync, sizeof(packet), 0, (struct sockaddr *) &leader_addr, serv_addr_len);
		printf("n: %d", n);		
		if (n < 0)
			handle_error("ERROR sendto\n");

		n = recvfrom(sockfd, &pckt_psync_rply, sizeof(packet), 0, (struct sockaddr *) &serv_addr, &serv_addr_len);
		if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
			handle_error("ERROR recvfrom\n");
		
		if (pckt_psync_rply.type == YOU_ARE_LEADER)
		{
			printf("got you are leader\n");
		}
		
		sleep(2);
	}
	while(n < 0 || (pckt_psync_rply.type != YOU_ARE_LEADER) || (pckt_psync_rply.type != I_AM_LEADER)); // SEGUIR PARA O PROCESSAMENTO

	// POTENTIALLY MOVE TO USE STATE MACHINE
	
	// WAIT FOR YOU ARE LEADER
	if (pckt_psync_rply.type == YOU_ARE_LEADER)
	{
you_are_leader_late:

		printf("GOT YOU ARE LEADER\n");

		//SET MY OWN ADDRESS
		my_addr =  server_list[leader_idx].serv_addr;
		server_state = LEADER;

		// SEND I AM LEADER
		for(int i = 0; i < num_servers; i++)
		{
			if (i != leader_idx)
			{
				packet pckt_am_leader, pckt_am_leader_ack;
				pckt_am_leader.type = I_AM_LEADER;
				pckt_am_leader.serv_addr = server_list[i].serv_addr;
				do
				{
					n = sendto(sockfd, &pckt_am_leader, sizeof(packet), 0, (struct sockaddr *) &server_list[i].serv_addr, sizeof(struct sockaddr_in));
					if (n < 0)
						handle_error("ERROR sendto\n");

					n = recvfrom(sockfd, &pckt_am_leader_ack, sizeof(packet), 0, (struct sockaddr *) &serv_addr, &serv_addr_len);
					if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
						handle_error("ERROR recvfrom\n");
					
				} while (n < 0 || !(pckt_am_leader_ack.type == AM_LEADER_ACK && serv_addr.sin_addr.s_addr == server_list[i].serv_addr.sin_addr.s_addr));
			}
			printf("RECEIVED AM LEADER ACK FROM: %s\n", inet_ntoa(serv_addr.sin_addr));
		}
	}

	// WAIT FOR I AM LEADER
	else if (pckt_psync_rply.type == I_AM_LEADER)
	{
am_leader_late:
		printf ("GOT I AM LEADER!\n");
		my_addr = pckt_psync_rply.serv_addr;

		packet pckt_am_leader_ack;
		pckt_am_leader_ack.type = AM_LEADER_ACK;
		do
		{
			n = sendto(sockfd, &pckt_am_leader_ack, sizeof(packet), 0, (struct sockaddr *) &server_list[leader_idx].serv_addr, sizeof(struct sockaddr_in));
			if (n < 0)
				handle_error("ERROR sendto\n");
			sleep(2);
		}while(1);
	}

	printf("End of leader stuff\n");
	
	while(1){}
	

	// PRINT INITIALIZATION MESSSAGE
	print_timestamp(); printf("num_reqs %lld total_sum %lld\n",shared_values.total_reqs, shared_values.total_sum);
		
	while (1) 
	{
		// WAIT FOR PACKETS
		n = recvfrom(sockfd, &pckt_cli, sizeof(packet), 0, (struct sockaddr *) &cli_addr, &clilen);
		if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
			handle_error("ERROR on recvfrom\n");
		
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

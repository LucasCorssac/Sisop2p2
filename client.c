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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <signal.h>

#include "packet.h"

void print_timestamp()
{
	time_t t = time(NULL);
 	struct tm tm = *localtime(&t);
  	printf("%d-%02d-%02d %02d:%02d:%02d ",
	 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}

int main(int argc, char *argv[])
{
	debug_print("Debug flag was defined\n");

    int sockfd, n;
	socklen_t serv_addr_len = sizeof(struct sockaddr_in);
	struct sockaddr_in brdcst_addr, serv_addr;
	int cli_port;
		
	char *broadcastIP;
	broadcastIP = "255.255.255.255"; 

	int broadcastPermission;
	
	packet pckt_disc, pckt_req, pckt_ack_disc, pckt_ack_req;

	#ifdef DEBUG
    	// GET PORT NUMBER
		if (argc < 3) {
			fprintf(stderr, "usage %s <port number> <\"id number\"> \n", argv[0]);
			exit(0);
		}
		sscanf(argv[1], "%d", &cli_port);
		debug_print("port given by user: %d\n", cli_port);

		// ADJUST ID
		int id;
		sscanf(argv[2], "%d", &id);

		debug_print("ID to be used by client: %d\n", id);
	#else 
		// GET PORT NUMBER
		if (argc < 2) {
			fprintf(stderr, "usage %s <port number>\n", argv[0]);
			exit(0);
		}
		sscanf(argv[1], "%d", &cli_port);
		debug_print("port given by user: %d\n", cli_port);
	#endif	
	

	
	// CREATE SOCKET
	if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1)
		handle_error("ERROR opening socket");

	// ADD BROADCAST PERMISSION 
	broadcastPermission = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_BROADCAST, (void *) &broadcastPermission,sizeof(broadcastPermission)) < 0)
		handle_error("setsockopt error");

	// SET BROADCAST ADDRESS
	memset(&brdcst_addr, 0, sizeof(brdcst_addr));
	brdcst_addr.sin_family = AF_INET;     
	brdcst_addr.sin_port = htons(cli_port);
	brdcst_addr.sin_addr.s_addr = inet_addr(broadcastIP);	 
	bzero(&(brdcst_addr.sin_zero), 8);

	/////// SEARCH FOR SERVER THROUGH BROADCASTING	
	int found_server = 0;
	
	pckt_disc.type = DISC;
	#ifdef DEBUG
    	pckt_disc.id = id;
	#endif

	// SET SOCKET REQUEST TIMEOUT PARAMETERS
	struct timeval timeout = {.tv_sec = 0, .tv_usec = 10000}; // 10 ms

	if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
		perror("setsockopt failed");
	}

	// SEARCH FOR SERVER
	while (!found_server)
	{
		// SEND BROADCAST MESSAGE
		n = sendto(sockfd, &pckt_disc, sizeof(packet), 0, (struct sockaddr *) &brdcst_addr, sizeof(brdcst_addr));
		if (n < 0)
			handle_error("ERROR sendto\n");
		
		// WAIT FOR SERVER ACK
		n = recvfrom(sockfd, &pckt_ack_disc, sizeof(packet), 0, (struct sockaddr *) &serv_addr, &serv_addr_len);
		if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
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
			handle_error("Error recvfrom\n");
		} 
		
		// CHECK IF SERVER SENT CORRECT RESPONSE
		if (pckt_ack_disc.type == DISC_ACK)
		{
			print_timestamp(); printf("server_addr %s\n", inet_ntoa(serv_addr.sin_addr));
			found_server = 1;			
		}
	}

	// INITIALIZE VARIABLES
	int 		 need_resend = 0;
	int 		 get_again = 0;
	int 		 value_to_send = -1;
	long long 	 seqn = 1;
	char *line = NULL;
    size_t len = 0;

	pckt_req.type = REQ;

	while (1)
	{
		if (!need_resend)
		{
			do 
			{
				if (getline(&line, &len, stdin) == -1) 
				{
					// EOF
					return 0;
				}
			} while(sscanf(line, "%d", &value_to_send) != 1); 
		}

		need_resend = 1; 				
		
		pckt_req.req.seqn = seqn;
		pckt_req.req.value = value_to_send;
		#ifdef DEBUG
    		pckt_req.id = id;
		#endif

		printf("Sending packet\n");

		sendto(sockfd, &pckt_req, sizeof(packet), 0, (struct sockaddr *) &serv_addr, serv_addr_len);

		debug_print("Waiting Response\n");

		do
		{
			if (recvfrom(sockfd, &pckt_ack_req, sizeof(packet), 0, (struct sockaddr *) &serv_addr, &serv_addr_len) == -1)
			{
				if (errno != EAGAIN && errno != EWOULDBLOCK) // Some unexpected error happened.
					handle_error("recv failed");
				else // timeout
				{
					debug_print("timeout!\n");
					get_again = 0;
				}
					
			}
			else
			{
				if (pckt_ack_req.type == REQ_ACK)  
				{
					if(pckt_ack_req.ack.seqn == seqn)
					{
						debug_print("REQ ACK received with no packet loss!\n");

						print_timestamp(); printf("server %s id_req %lld value %lld num_reqs %lld total_sum %lld\n",
												inet_ntoa(serv_addr.sin_addr),
												pckt_ack_req.ack.seqn,
												pckt_ack_req.ack.value,
												pckt_ack_req.ack.num_reqs,
												pckt_ack_req.ack.total_sum);

						need_resend = 0;
						get_again = 0;
						seqn++;
					}
					else if (pckt_ack_req.ack.seqn < seqn) // Received ACK from an already treated message
					{
						get_again = 1;
					}
				}
			}
		} while (get_again);
		
	}

	close(sockfd);
	return 0;
}

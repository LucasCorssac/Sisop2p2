typedef enum PACKET_TYPE {
    NULLPCKT,
    DISC,
    REQ,
    DISC_ACK,
    REQ_ACK,
    SERV_DISC,
    SERV_FOUND_ALL,
    YOU_ARE_LEADER,
    I_AM_LEADER,
    AM_LEADER_ACK,
    DISC_REP,
    DISC_REP_ACK,
    HEARTBEAT,
    ELECTION,
    ELECTION_WAIT
} packet_type;

struct requisicao
{
    long long seqn;
    long long value;
};
struct requisicao_ack
{
    long long seqn; 
    long long value;
    long long num_reqs;
    long long total_sum;
};
typedef struct __packet
{
    #ifdef DEBUG
        int id;
    #endif
    packet_type type;
    union
    {
        struct sockaddr_in serv_addr;
        struct sockaddr_in cli_addr;
        struct requisicao req;
        struct requisicao_ack ack;
    };
} packet;

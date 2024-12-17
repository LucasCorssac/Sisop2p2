typedef enum PACKET_TYPE {
    DISC,
    REQ,
    DISC_ACK,
    REQ_ACK 
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
        struct requisicao req;
        struct requisicao_ack ack;
    };
} packet;

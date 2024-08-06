#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <iostream>
#include <string>
//#include "utils.h"

static void dump_row(long count, int numinrow, int *chs) {
  int i;

  printf("%08lX:", count - numinrow);

  if (numinrow > 0) {
    for (i = 0; i < numinrow; i++) {
      if (i == 8) {
        printf(" :");
      }
      printf(" %02X", chs[i]);
    }
    for (i = numinrow; i < 16; i++) {
      if (i == 8) {
        printf(" :");
      }
      printf("   ");
    }
    printf("  ");
    for (i = 0; i < numinrow; i++) {
      if (isprint(chs[i])) {
        printf("%c", chs[i]);
      } else {
        printf(".");
      }
    }
  }
  printf("\n");
}

static int rows_eq(int *a, int *b) {
  int i;

  for (i = 0; i < 16; i++)
    if (a[i] != b[i]) {
      return 0;
    }
  return 1;
}
void amqp_dump(void const *buffer, size_t len) {
  unsigned char *buf = (unsigned char *)buffer;
  long count = 0;
  int numinrow = 0;
  int chs[16];
  int oldchs[16] = {0};
  int showed_dots = 0;
  size_t i;

  for (i = 0; i < len; i++) {
    int ch = buf[i];

    if (numinrow == 16) {
      int j;

      if (rows_eq(oldchs, chs)) {
        if (!showed_dots) {
          showed_dots = 1;
          printf(
              "          .. .. .. .. .. .. .. .. : .. .. .. .. .. .. .. ..\n");
        }
      } else {
        showed_dots = 0;
        dump_row(count, numinrow, chs);
      }

      for (j = 0; j < 16; j++) {
        oldchs[j] = chs[j];
      }

      numinrow = 0;
    }

    count++;
    chs[numinrow++] = ch;
  }

  dump_row(count, numinrow, chs);

  if (numinrow != 0) {
    printf("%08lX:\n", count);
  }
}


void check_amqp_reply(amqp_rpc_reply_t reply) {
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "AMQP operation failed: " << reply.reply_type << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

void log_amqp_reply(amqp_rpc_reply_t reply) {
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "AMQP operation failed with reply type: " << reply.reply_type << std::endl;
        switch (reply.reply_type) {
            
            case AMQP_RESPONSE_SERVER_EXCEPTION:
                std::cerr << "Server exception occurred" << std::endl;
                break;
            default:
                std::cerr << "Unknown reply type" << std::endl;
                break;
        }
    }
}

int main() {
    const std::string hostname = "localhost";
    const int port = 5672;
    const std::string queue_name = "priority_queue";

    // Setup connection
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);

    if (!socket) {
        std::cerr << "Failed to create TCP socket" << std::endl;
        return EXIT_FAILURE;
    }

    if (amqp_socket_open(socket, hostname.c_str(), port)) {
        std::cerr << "Failed to open TCP socket" << std::endl;
        return EXIT_FAILURE;
    }
    //logging into rabbitmq server
    amqp_rpc_reply_t reply = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    check_amqp_reply(reply);

    amqp_channel_open(conn, 1);
    reply = amqp_get_rpc_reply(conn);
    check_amqp_reply(reply);

    
    amqp_bytes_t queue = amqp_cstring_bytes(queue_name.c_str());
    /**
 * amqp_queue_declare
 *
 * @param [in] state connection state
 * @param [in] channel the channel to do the RPC on
 * @param [in] queue queue
 * @param [in] passive passive
 * @param [in] durable durable
 * @param [in] exclusive exclusive
 * @param [in] auto_delete auto_delete
 * @param [in] arguments arguments
 * @returns amqp_queue_declare_ok_t
 */
    // Declare a durable, priority-enabled queue
    amqp_queue_declare(conn, 1, queue, 0, 1, 0, 0, amqp_empty_table);
    reply = amqp_get_rpc_reply(conn);
    check_amqp_reply(reply);

/**
 * amqp_queue_bind
 *
 * @param [in] state connection state
 * @param [in] channel the channel to do the RPC on
 * @param [in] queue queue
 * @param [in] exchange exchange
 * @param [in] routing_key routing_key
 * @param [in] arguments arguments
 * @returns amqp_queue_bind_ok_t
 */

    amqp_queue_bind(conn, 1, queue,amqp_cstring_bytes("amq.direct"), amqp_cstring_bytes("info"), amqp_empty_table);

    
    /**
 * amqp_basic_consume
 *
 * @param [in] state connection state
 * @param [in] channel the channel to do the RPC on
 * @param [in] queue queue
 * @param [in] consumer_tag consumer_tag
 * @param [in] no_local no_local
 * @param [in] no_ack no_ack
 * @param [in] exclusive exclusive
 * @param [in] arguments arguments
 * @returns amqp_basic_consume_ok_t
 */
  // Start consuming messages
    amqp_basic_consume(conn, 1, queue, amqp_cstring_bytes("CDP"), 0, 0, 0, amqp_empty_table);
    reply = amqp_get_rpc_reply(conn);
    check_amqp_reply(reply);



    std::cout << "Waiting for messages from queue: " << queue_name << std::endl;

    
   for (;;) {
      amqp_rpc_reply_t res;
      amqp_envelope_t envelope;

      amqp_maybe_release_buffers(conn);

      res = amqp_consume_message(conn, &envelope, NULL, 0);

      if (AMQP_RESPONSE_NORMAL != res.reply_type) {
        break;
      }

      printf("Delivery %u, exchange %.*s routingkey %.*s\n",
             (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
             (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
             (char *)envelope.routing_key.bytes);

      if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        printf("Content-type: %.*s\n",
               (int)envelope.message.properties.content_type.len,
               (char *)envelope.message.properties.content_type.bytes);
      }
      printf("----\n");
      amqp_basic_ack(conn, 1, envelope.delivery_tag, 0);

      amqp_dump(envelope.message.body.bytes, envelope.message.body.len);
      
      amqp_destroy_envelope(&envelope);
    }

    // Cleanup
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return EXIT_SUCCESS;
}
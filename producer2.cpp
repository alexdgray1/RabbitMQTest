#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

void check_amqp_reply(amqp_rpc_reply_t reply) {
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "AMQP operation failed: " << reply.reply_type << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

int main() {
    const std::string hostname = "localhost";
    const int port = 5672;
    const std::string queue_name = "priority_queue2";

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

    amqp_rpc_reply_t reply = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    check_amqp_reply(reply);

    amqp_channel_open(conn, 2);
    reply = amqp_get_rpc_reply(conn);
    check_amqp_reply(reply);

    // Declare a durable queue
    amqp_bytes_t queue = amqp_cstring_bytes(queue_name.c_str());
    amqp_queue_declare(conn, 2, queue, 0, 1, 0, 0, amqp_empty_table);
    reply = amqp_get_rpc_reply(conn);
    check_amqp_reply(reply);

    std::cout << "Queue declared successfully." << std::endl;

    // Publish a simple message
    while(true)
    {
        std::string message = "hello hello";
        amqp_bytes_t message_body = amqp_cstring_bytes(message.c_str());
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("from CDP");
        props.delivery_mode = 2; //allows survival of message in case broker restarts

        //sets up exchange and routing key
        amqp_basic_publish(conn, 2, amqp_cstring_bytes("amq.direct"),amqp_cstring_bytes("more info"),  0, 0, &props, message_body);
        reply = amqp_get_rpc_reply(conn);
        check_amqp_reply(reply);

        std::cout << "Message sent: " << message << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(5));
        

    }

    // Cleanup
    amqp_channel_close(conn, 2, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return EXIT_SUCCESS;
}
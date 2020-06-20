package Streaming;

public interface Constants {
    public static String KAFKA_BROKERS = "localhost:9092";
    public static String CLIENT_ID = "client1";
    public static String SESSION_TIMEOUT_MS = "30000";
    public static String GROUP_ID_CONFIG = "consumerGroup1";
    public static String TOPIC_NAME = "topicB";
    public static String OFFSET_RESET_EARLIER="earliest";
    public static String OFFSET_RESET_LATEST="latest";
    public static String ENABLE_AUTO_COMMIT = "false";
    public static Integer MAX_POLL_RECORDS = 1;
}

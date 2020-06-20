package util;

public interface Constants {
    public static String KAFKA_BROKERS = "localhost:9092";
    public static String CLIENT_ID = "client1";
    public static String ACKS = "all";
    public static int RETRIES = 0;
    public static int BATCH_SIZE = 16384;
    public static String ENABLE_AUTO_COMMIT = "true";
    public static String AUTO_COMMIT_INTERVAL_MS = "1000";
    public static String SESSION_TIMEOUT_MS = "30000";
    public static String GROUP_ID_CONFIG = "consumerGroup1";
    public static String TOPIC_NAME = "topicB";
}

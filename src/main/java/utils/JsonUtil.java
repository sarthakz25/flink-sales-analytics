package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dto.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    public static String convertTransactionToJson(Transaction transaction) {
        if (transaction == null) {
            logger.warn("Attempted to convert null transaction to JSON");
            return null;
        }

        try {
            return objectMapper.writeValueAsString(transaction);
        } catch (JsonProcessingException e) {
            logger.error("Error converting transaction to JSON", e);
            return null;
        }
    }
}

package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.*;
import static org.apache.kafka.common.protocol.types.Type.*;

public class NackProduceResponse extends AbstractResponse {
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String TRANSACTIONAL_ID = "transactional id";

    private static final Schema NACK_PRODUCE_REQUEST_V0 = new Schema(
            new Field(TIMEOUT_KEY_NAME, INT32, "The time to await a response in ms."),
            new Field(TRANSACTIONAL_ID, NULLABLE_STRING, "Transactional id.")
    );

    // If more schemas are implemented
    public static Schema[] schemaVersions() {
        return new Schema[] {NACK_PRODUCE_REQUEST_V0};
    }

    private final Errors error;
    private final String transactionalId;
    private final int throttleTimeMs;

    public NackProduceResponse(Errors error, int throttleTimeMs, String transactionalId) {
        this.error = error;
        this.throttleTimeMs = throttleTimeMs;
        this.transactionalId = transactionalId;
    }

    public Errors error() {
        return error;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    public Struct toStruct(short version){
        Struct struct = new Struct(ApiKeys.API_VERSIONS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(TIMEOUT_KEY_NAME, throttleTimeMs);
        struct.set(TRANSACTIONAL_ID, transactionalId);
        struct.set(ERROR_CODE, error.code());
        return struct;
    }
}
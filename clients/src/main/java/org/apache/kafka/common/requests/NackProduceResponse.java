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
    private static final String ERROR_CODE = "Codigo de um erro mano, codigo de um erro.";

    private static final Schema NACK_PRODUCE_REQUEST_V0 = new Schema(
            new Field(TIMEOUT_KEY_NAME, INT32, "The time to await a response in ms."),
            new Field(TRANSACTIONAL_ID, NULLABLE_STRING, "Transactional id."),
            new Field(ERROR_CODE, INT64, "Error code.")
    );

    // If more schemas are implemented
    public static Schema[] schemaVersions() {
        return new Schema[] {NACK_PRODUCE_REQUEST_V0};
    }

    private final Errors error;
    private final String transactionalId;
    private final int timeout;

    public NackProduceResponse(Errors error, int timeout, String transactionalId) {
        this.error = error;
        this.timeout = timeout;
        this.transactionalId = transactionalId;
    }

    public NackProduceResponse(Struct struct) {
        this.error = Errors.forCode(struct.getShort(ERROR_CODE));
        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
        this.transactionalId = struct.getString(TRANSACTIONAL_ID);
    }

    public Errors error() {
        return error;
    }

    public int timeout() {
        return timeout;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    public Struct toStruct(short version){
        Struct struct = new Struct(ApiKeys.API_VERSIONS.responseSchema(version));
        struct.set(TIMEOUT_KEY_NAME, timeout);
        struct.set(TRANSACTIONAL_ID, transactionalId);
        struct.set(ERROR_CODE, error.code());
        return struct;
    }
}
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.protocol.types.Type.*;

public class NackProduceRequest extends AbstractRequest {
    private static final String ACKS_KEY_NAME = "acks";
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String PRODUCER_ID = "producerId";

    private static final Schema NACK_PRODUCE_REQUEST_V0 = new Schema(
            new Field(ACKS_KEY_NAME, INT16, "Ack que que sera utilizado para a produção com nack."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time to await a response in ms."),
            new Field(PRODUCER_ID, INT32, "Identification of producer.")
    );

    // If more schemas are implemented
    public static Schema[] schemaVersions() {
        return new Schema[] {NACK_PRODUCE_REQUEST_V0};
    }

    public static class Builder extends AbstractRequest.Builder<NackProduceRequest> {
        private final short acks;
        private final int timeout;
        private final int producerId;

        public Builder (short version, short acks, int timeout, int producerId) {
            super(ApiKeys.NACK_PRODUCE_REQUEST, version);
            this.acks = acks;
            this.timeout = timeout;
            this.producerId = producerId;
        }

        @Override
        public NackProduceRequest build(short version) {
            return new NackProduceRequest(version, acks, timeout, producerId);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=NackRequest")
                    .append(", acks=").append(acks)
                    .append(", timeout=").append(timeout)
                    .append("), producerId='").append(producerId)
                    .append("'");
            return bld.toString();
        }
    }

    private final short acks;
    private final int timeout;
    private final int producerId;

    private NackProduceRequest(short version, short acks, int timeout,
                               int producerId) {
        super(ApiKeys.NACK_PRODUCE_REQUEST, version);
        this.acks = acks;
        this.timeout = timeout;
        this.producerId = producerId;
    }
    // TODO implementar método contrutor que receba uma struct como parâmetro.
    public NackProduceRequest(Struct struct, short version) {
        super(ApiKeys.NACK_PRODUCE_REQUEST, version);

        this.acks = struct.getShort(ACKS_KEY_NAME);
        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
        this.producerId = struct.getInt(PRODUCER_ID);
    }

    @Override
    public Struct toStruct(){
        short version = version();
        Struct struct = new Struct(ApiKeys.NACK_PRODUCE_REQUEST.requestSchema(version));

        struct.set(ACKS_KEY_NAME, acks);
        struct.set(TIMEOUT_KEY_NAME, timeout);
        struct.set(PRODUCER_ID, producerId);

        return struct;
    }

    @Override
    public NackProduceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        // Alterar pra um formato de NackProduceResponse
        return null;
    }

    @Override
    public Map<Errors, Integer> errorCounts(Throwable e) {
        Errors error = Errors.forException(e);
        return Collections.singletonMap(error, 1);
    }

    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

}
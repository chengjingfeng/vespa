// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package ai.vespa.feed.client;

import java.util.Optional;

/**
 * @author bjorncs
 * @author jonmv
 */
public class Result {

    private final Type type;
    private final DocumentId documentId;
    private final String resultMessage;
    private final String traceMessage;

    Result(Type type, DocumentId documentId, String resultMessage, String traceMessage) {
        this.type = type;
        this.documentId = documentId;
        this.resultMessage = resultMessage;
        this.traceMessage = traceMessage;
    }

    public enum Type {
        success,
        conditionNotMet,
        failure
    }

    public Type type() { return type; }
    public DocumentId documentId() { return documentId; }
    public Optional<String> resultMessage() { return Optional.ofNullable(resultMessage); }
    public Optional<String> traceMessage() { return Optional.ofNullable(traceMessage); }

}

package graphql.execution;

import com.google.common.collect.ImmutableList;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.PublicApi;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static graphql.Assert.assertNotNull;

@PublicApi
public class FieldValueInfo {

    public enum CompleteValueType {
        OBJECT,
        LIST,
        NULL,
        SCALAR,
        ENUM

    }

    private final CompleteValueType completeValueType;
    private final Object /* CompletableFuture<Object> | Object */ fieldValueObject;
    private final List<FieldValueInfo> fieldValueInfos;

    public FieldValueInfo(CompleteValueType completeValueType, Object fieldValueObject) {
        this(completeValueType, fieldValueObject, ImmutableList.of());
    }

    public FieldValueInfo(CompleteValueType completeValueType, Object fieldValueObject, List<FieldValueInfo> fieldValueInfos) {
        assertNotNull(fieldValueInfos, "fieldValueInfos can't be null");
        this.completeValueType = completeValueType;
        this.fieldValueObject = fieldValueObject;
        this.fieldValueInfos = fieldValueInfos;
    }

    public CompleteValueType getCompleteValueType() {
        return completeValueType;
    }

    public Object /* CompletableFuture<Object> | Object */ getFieldValueObject() {
        return fieldValueObject;
    }

    /**
     * This returns the value in {@link CompletableFuture} form.  If it is already a {@link CompletableFuture} it is returned
     * directly, otherwise the materialized value is wrapped in a {@link CompletableFuture} and returned
     *
     * @return a {@link CompletableFuture} promise to the value
     */
    public CompletableFuture<Object> getFieldValueFuture() {
        return Async.toCompletableFuture(fieldValueObject);
    }

    /**
     * Kept for legacy reasons - this method is no longer sensible and is no longer used by the graphql-java engine
     * and is kept only for backwards compatible API reasons.
     *
     * @return a promise to the {@link ExecutionResult} that wraps the field value.
     */
    @Deprecated
    public CompletableFuture<ExecutionResult> getFieldValue() {
        return getFieldValueFuture().thenApply(fv -> ExecutionResultImpl.newExecutionResult().data(fv).build());
    }

    public List<FieldValueInfo> getFieldValueInfos() {
        return fieldValueInfos;
    }

    @Override
    public String toString() {
        return "FieldValueInfo{" +
                "completeValueType=" + completeValueType +
                ", fieldValue=" + fieldValueObject +
                ", fieldValueInfos=" + fieldValueInfos +
                '}';
    }
}
package graphql.execution;

import graphql.Assert;
import graphql.Internal;
import graphql.collect.ImmutableKit;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

@Internal
@SuppressWarnings("FutureReturnValueIgnored")
public class Async {

    public interface CombinedBuilder<T> {

        void add(CompletableFuture<T> completableFuture);

        CompletableFuture<List<T>> await();

        /**
         * This will return a {@code CompletableFuture<List<T>>} if ANY of the input values are async
         * otherwise it just return a materialised {@code List<T>}
         *
         * @return either a CompletableFuture or a materialized list
         */
        /* CompletableFuture<List<T>> | List<T> */ Object awaitPolymorphic();

        /**
         * This adds a new value which can be either a materialized value or a {@link CompletableFuture}
         *
         * @param object the object to add
         */
        void addObject(Object object);
    }

    /**
     * Combines 1 or more CF. It is a wrapper around CompletableFuture.allOf.
     *
     * @param expectedSize how many we expect
     * @param <T>          for two
     *
     * @return a combined builder of CFs
     */
    public static <T> CombinedBuilder<T> ofExpectedSize(int expectedSize) {
        if (expectedSize == 0) {
            return new Empty<>();
        } else if (expectedSize == 1) {
            return new Single<>();
        } else {
            return new Many<>(expectedSize);
        }
    }

    private static class Empty<T> implements CombinedBuilder<T> {

        private int ix;

        @Override
        public void add(CompletableFuture<T> completableFuture) {
            this.ix++;
        }

        @Override
        public void addObject(Object object) {
            this.ix++;
        }


        @Override
        public CompletableFuture<List<T>> await() {
            Assert.assertTrue(ix == 0, () -> "expected size was " + 0 + " got " + ix);
            return typedEmpty();
        }

        @Override
        public Object awaitPolymorphic() {
            Assert.assertTrue(ix == 0, () -> "expected size was " + 0 + " got " + ix);
            return Collections.emptyList();
        }

        private static final CompletableFuture<List<?>> EMPTY = CompletableFuture.completedFuture(Collections.emptyList());

        @SuppressWarnings("unchecked")
        private static <T> CompletableFuture<T> typedEmpty() {
            return (CompletableFuture<T>) EMPTY;
        }
    }

    private static class Single<T> implements CombinedBuilder<T> {

        // avoiding array allocation as there is only 1 CF
        private Object value;
        private int ix;

        @Override
        public void add(CompletableFuture<T> completableFuture) {
            this.value = completableFuture;
            this.ix++;
        }

        @Override
        public void addObject(Object object) {
            this.value = object;
            this.ix++;
        }

        @Override
        public Object awaitPolymorphic() {
            commonSizeAssert();
            if (value instanceof CompletableFuture) {
                @SuppressWarnings("unchecked")
                CompletableFuture<T> cf = (CompletableFuture<T>) value;
                return cf.thenApply(Collections::singletonList);
            }
            //noinspection unchecked
            return Collections.singletonList((T) value);
        }

        @Override
        public CompletableFuture<List<T>> await() {
            commonSizeAssert();
            if (value instanceof CompletableFuture) {
                @SuppressWarnings("unchecked")
                CompletableFuture<T> cf = (CompletableFuture<T>) value;
                return cf.thenApply(Collections::singletonList);
            }
            //noinspection unchecked
            return CompletableFuture.completedFuture(Collections.singletonList((T) value));
        }

        private void commonSizeAssert() {
            Assert.assertTrue(ix == 1, () -> "expected size was " + 1 + " got " + ix);
        }
    }

    private static class Many<T> implements CombinedBuilder<T> {

        private final Object[] array;
        private int ix;
        private int cfCount;

        private Many(int size) {
            this.array = new Object[size];
            this.ix = 0;
            cfCount = 0;
        }

        @Override
        public void add(CompletableFuture<T> completableFuture) {
            array[ix++] = completableFuture;
            cfCount++;
        }

        @Override
        public void addObject(Object object) {
            array[ix++] = object;
            if (object instanceof CompletableFuture) {
                cfCount++;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public CompletableFuture<List<T>> await() {
            commonSizeAssert();

            CompletableFuture<List<T>> overallResult = new CompletableFuture<>();
            if (cfCount == 0) {
                overallResult.complete(materialisedList(array));
            } else {
                CompletableFuture<T>[] cfsArr = copyOnlyCFsToArray();
                CompletableFuture.allOf(cfsArr)
                        .whenComplete((ignored, exception) -> {
                            if (exception != null) {
                                overallResult.completeExceptionally(exception);
                                return;
                            }
                            List<T> results = new ArrayList<>(array.length);
                            if (cfsArr.length == array.length) {
                                // they are all CFs
                                for (CompletableFuture<T> cf : cfsArr) {
                                    results.add(cf.join());
                                }
                            } else {
                                // it's a mixed bag of CFs and materialized objects
                                for (Object object : array) {
                                    if (object instanceof CompletableFuture) {
                                        CompletableFuture<T> cf = (CompletableFuture<T>) object;
                                        // join is safe since they are all completed earlier via CompletableFuture.allOf()
                                        results.add(cf.join());
                                    } else {
                                        results.add((T) object);
                                    }
                                }
                            }
                            overallResult.complete(results);
                        });
            }
            return overallResult;
        }

        @Override
        public Object awaitPolymorphic() {
            if (cfCount == 0) {
                commonSizeAssert();
                return materialisedList(array);
            } else {
                return await();
            }
        }

        @SuppressWarnings("unchecked")
        @NotNull
        private CompletableFuture<T>[] copyOnlyCFsToArray() {
            if (cfCount == array.length) {
                // if it's all CFs - make a type safe copy via C code
                return Arrays.copyOf(array, array.length, CompletableFuture[].class);
            } else {
                int i = 0;
                CompletableFuture<T>[] dest = new CompletableFuture[cfCount];
                for (Object o : array) {
                    if (o instanceof CompletableFuture) {
                        dest[i] = (CompletableFuture<T>) o;
                        i++;
                    }
                }
                return dest;
            }
        }

        @NotNull
        private List<T> materialisedList(Object[] array) {
            List<T> results = new ArrayList<>(array.length);
            for (Object object : array) {
                //noinspection unchecked
                results.add((T) object);
            }
            return results;
        }

        private void commonSizeAssert() {
            Assert.assertTrue(ix == array.length, () -> "expected size was " + array.length + " got " + ix);
        }

    }

    @FunctionalInterface
    public interface CFFactory<T, U> {
        CompletableFuture<U> apply(T input, int index, List<U> previousResults);
    }

    public static <U> CompletableFuture<List<U>> each(List<CompletableFuture<U>> futures) {
        CompletableFuture<List<U>> overallResult = new CompletableFuture<>();

        @SuppressWarnings("unchecked")
        CompletableFuture<U>[] arrayOfFutures = futures.toArray(new CompletableFuture[0]);
        CompletableFuture
                .allOf(arrayOfFutures)
                .whenComplete((ignored, exception) -> {
                    if (exception != null) {
                        overallResult.completeExceptionally(exception);
                        return;
                    }
                    List<U> results = new ArrayList<>(arrayOfFutures.length);
                    for (CompletableFuture<U> future : arrayOfFutures) {
                        results.add(future.join());
                    }
                    overallResult.complete(results);
                });
        return overallResult;
    }

    public static <T, U> CompletableFuture<List<U>> each(Collection<T> list, BiFunction<T, Integer, CompletableFuture<U>> cfFactory) {
        List<CompletableFuture<U>> futures = new ArrayList<>(list.size());
        int index = 0;
        for (T t : list) {
            CompletableFuture<U> cf;
            try {
                cf = cfFactory.apply(t, index++);
                Assert.assertNotNull(cf, () -> "cfFactory must return a non null value");
            } catch (Exception e) {
                cf = new CompletableFuture<>();
                // Async.each makes sure that it is not a CompletionException inside a CompletionException
                cf.completeExceptionally(new CompletionException(e));
            }
            futures.add(cf);
        }
        return each(futures);

    }

    /**
     * This will run the value factory for each of the values in the provided list.
     * <p>
     * If any of the values provided is a {@link CompletableFuture} it will return a {@link CompletableFuture} result object
     * that joins on all values otherwise if none of the values are a {@link CompletableFuture} then it will return a materialized list.
     *
     * @param list                         the list to work over
     * @param cfOrMaterialisedValueFactory the value factory to call for each iterm in the list
     * @param <T>                          for two
     *
     * @return a {@link CompletableFuture} to the list of resolved values or the list of values in a materialized fashion
     */
    public static <T> /* CompletableFuture<List<U>> | List<U> */ Object eachPolymorphic(Collection<T> list, Function<T, Object> cfOrMaterialisedValueFactory) {
        CombinedBuilder<Object> futures = ofExpectedSize(list.size());
        for (T t : list) {
            try {
                Object value = cfOrMaterialisedValueFactory.apply(t);
                futures.addObject(value);
            } catch (Exception e) {
                CompletableFuture<Object> cf = new CompletableFuture<>();
                // Async.each makes sure that it is not a CompletionException inside a CompletionException
                cf.completeExceptionally(new CompletionException(e));
                futures.add(cf);
            }
        }
        return futures.awaitPolymorphic();
    }

    public static <T, U> CompletableFuture<List<U>> eachSequentially(Iterable<T> list, BiFunction<T, List<U>, Object> cfOrMaterialisedValueFactory) {
        CompletableFuture<List<U>> result = new CompletableFuture<>();
        eachSequentiallyPolymorphicImpl(list.iterator(), cfOrMaterialisedValueFactory, new ArrayList<>(), result);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <T, U> void eachSequentiallyPolymorphicImpl(Iterator<T> iterator, BiFunction<T, List<U>, Object> cfOrMaterialisedValueFactory, List<U> tmpResult, CompletableFuture<List<U>> overallResult) {
        if (!iterator.hasNext()) {
            overallResult.complete(tmpResult);
            return;
        }
        Object value;
        try {
            value = cfOrMaterialisedValueFactory.apply(iterator.next(), tmpResult);
        } catch (Exception e) {
            overallResult.completeExceptionally(new CompletionException(e));
            return;
        }
        if (value instanceof CompletableFuture) {
            CompletableFuture<U> cf = (CompletableFuture<U>) value;
            cf.whenComplete((cfResult, exception) -> {
                if (exception != null) {
                    overallResult.completeExceptionally(exception);
                    return;
                }
                tmpResult.add(cfResult);
                eachSequentiallyPolymorphicImpl(iterator, cfOrMaterialisedValueFactory, tmpResult, overallResult);
            });
        } else {
            tmpResult.add((U) value);
            eachSequentiallyPolymorphicImpl(iterator, cfOrMaterialisedValueFactory, tmpResult, overallResult);
        }
    }

    private static <T, U> void eachSequentiallyImpl(Iterator<T> iterator, CFFactory<T, U> cfFactory, int index, List<U> tmpResult, CompletableFuture<List<U>> overallResult) {
        if (!iterator.hasNext()) {
            overallResult.complete(tmpResult);
            return;
        }
        CompletableFuture<U> cf;
        try {
            cf = cfFactory.apply(iterator.next(), index, tmpResult);
            Assert.assertNotNull(cf, () -> "cfFactory must return a non null value");
        } catch (Exception e) {
            cf = new CompletableFuture<>();
            cf.completeExceptionally(new CompletionException(e));
        }
        cf.whenComplete((cfResult, exception) -> {
            if (exception != null) {
                overallResult.completeExceptionally(exception);
                return;
            }
            tmpResult.add(cfResult);
            eachSequentiallyImpl(iterator, cfFactory, index + 1, tmpResult, overallResult);
        });
    }


    /**
     * Turns an object T into a CompletableFuture if it's not already
     *
     * @param t   - the object to check
     * @param <T> for two
     *
     * @return a CompletableFuture
     */
    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> toCompletableFuture(Object t) {
        if (t instanceof CompletionStage) {
            //noinspection unchecked
            return ((CompletionStage<T>) t).toCompletableFuture();
        } else {
            return CompletableFuture.completedFuture((T) t);
        }
    }

    /**
     * Turns a CompletionStage into a CompletableFuture if it's not already, otherwise leaves it alone
     * as a materialized object.
     *
     * @param object - the object to check
     *
     * @return a CompletableFuture from a CompletionStage or the materialized object itself
     */
    public static Object toCompletableFutureOrMaterializedObject(Object object) {
        if (object instanceof CompletionStage) {
            return ((CompletionStage<?>) object).toCompletableFuture();
        } else {
            return object;
        }
    }

    public static <T> CompletableFuture<T> tryCatch(Supplier<CompletableFuture<T>> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            CompletableFuture<T> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }
    }

    public static <T> CompletableFuture<T> exceptionallyCompletedFuture(Throwable exception) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(exception);
        return result;
    }

    public static <U, T> CompletableFuture<List<U>> flatMap(List<T> inputs, Function<T, CompletableFuture<U>> mapper) {
        List<CompletableFuture<U>> collect = ImmutableKit.map(inputs, mapper);
        return Async.each(collect);
    }

    public static <U, T> CompletableFuture<List<U>> map(CompletableFuture<List<T>> values, Function<T, U> mapper) {
        return values.thenApply(list -> ImmutableKit.map(list, mapper));
    }

    public static <U, T> List<CompletableFuture<U>> map(List<CompletableFuture<T>> values, Function<T, U> mapper) {
        return ImmutableKit.map(values, cf -> cf.thenApply(mapper));
    }

    public static <U, T> List<CompletableFuture<U>> mapCompose(List<CompletableFuture<T>> values, Function<T, CompletableFuture<U>> mapper) {
        return ImmutableKit.map(values, cf -> cf.thenCompose(mapper));
    }

}

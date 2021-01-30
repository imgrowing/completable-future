package ex;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Slf4j
public class TwentyExamplesTest {

    private static final Random RANDOM = new Random();
    private static final int BASE_SLEEP_MS = 100;
    private static final int MAX_SLEEP_MS = 100;

    private final ExecutorService EXECUTOR = Executors.newFixedThreadPool(3, new ThreadFactory() {
        private int count = 1;

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "custom-executor-" + count++);
        }
    }) ;

    /**
     * completed 된 CompletableFuture 생성하기
     */
    @Test
    public void completedFutureExample() {
        CompletableFuture<String> cf = CompletableFuture.completedFuture("message");

        // isDone() - completed 되었으면(정상, exceptionally, cancellation 모두) true를 반환한다.
        assertThat(cf.isDone()).isTrue();

        // getNow(T: valueIfAbsent): T - 완료된 경우 결과값을, 완료되지 않았으면 valueIfAbsent 를 반환한다.
        assertThat(cf.getNow(null)).isEqualTo("message");
    }

    /**
     * Runnable을 비동기로 실행하기
     * 메소드가 "Async" 로 끝나면 비동기로 실행된다는 의미이다.
     * 별도의 executor를 지정하지 않으면 공용 ForkJoinPool의 스레드에서 실행된다.
     */
    @Test
    public void runAsyncExample() {
        CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
            assertThat(isCommonForkJoinPoolThread()).isTrue();
            randomSleep();
        });

        // randomSleep() 중이어서 아직 완료되지 않았다.
        assertThat(cf.isDone()).isFalse();

        sleepEnough();

        assertThat(cf.isDone()).isTrue();
    }

    /**
     * 앞 단계(sync)의 결과에 Function 을 sync로 적용하기
     * "then"은 앞 단계에서 에러 없이 정상 완료될 때, 어떤 action을 수행하겠다는 의미
     * "Apply"는 앞 단계의 결과에 Function을 적용(apply) 하겠다는 의미
     * thenApply(function: Function<T, R>): CompletionStage<R>
     * thenApply는 sync로 동작하므로 이 라인에서 blocking 되며, thenApply의 처리가 끝나야 다음 라인으로 진행한다.
     */
    @Test
    public void thenApplyExample() {
        CompletableFuture<String> cf = CompletableFuture.completedFuture("message")
                .thenApply(s -> {
                    assertThat(isCommonForkJoinPoolThread()).isFalse();
                    return s.toUpperCase();
                });

        assertThat(cf.getNow(null)).isEqualTo("MESSAGE");
    }

    /**
     * 앞 단계(sync)의 결과에 Function 을 async로 적용하기
     * 메소드가 "Async" 로 끝나면 비동기로 실행된다는 의미이다.
     * 별도의 executor를 지정하지 않으면 공용 ForkJoinPool의 스레드에서 실행된다.
     */
    @Test
    public void thenApplyAsyncExample() {
        CompletableFuture<String> cf = CompletableFuture.completedFuture("message")
                .thenApplyAsync(s -> {
                    assertThat(isCommonForkJoinPoolThread()).isTrue();
                    randomSleep();
                    return s.toUpperCase();
                });

        // 아직 완료되지 않아 결과값이 존재하지 않는다.
        assertThat(cf.getNow(null)).isNull();

        // 완료되었다면 결과값을 반환하고, 그렇지 않으면 완료될 때 까지 기다린다.
        assertThat(cf.join()).isEqualTo("MESSAGE");
    }

    /**
     * 앞 단계(sync)의 결과에 Function 을 async로 적용한다. 별도의 executor를 사용한다.
     * 별도의 executor를 지정하면 해당 스레드 pool에서 실행된다.
     */
    @Test
    public void thenApplyAsyncWithExecutorExample() {
        CompletableFuture<String> cf = CompletableFuture.completedFuture("message")
                .thenApplyAsync(
                        s -> {
                            assertThat(isCustomExecutorThread()).isTrue();
                            assertThat(isCommonForkJoinPoolThread()).isFalse();
                            randomSleep();
                            return s.toUpperCase();
                        },
                        EXECUTOR
                );

        assertThat(cf.getNow(null)).isNull();
        assertThat(cf.join()).isEqualTo("MESSAGE");
    }

    /**
     * 앞 단계(sync)의 결과에 Consumer 를 sync로 적용하기
     * "Accept"는 앞 단계의 결과에 Consumer를 적용(accept) 하겠다는 의미. 반환값이 Void 이다.
     * thenAccept(consumer: Consumer<T>): CompletionStage<Void>
     */
    @Test
    public void thenAcceptExample() {
        StringBuilder result = new StringBuilder();
        CompletableFuture.completedFuture("thenAccept message")
                .thenAccept(s -> result.append(s));

        assertThat(result.length()).isGreaterThan(0);
    }

    /**
     * 앞 단계(sync)의 결과에 Consumer 을 async로 적용하기
     * 메소드가 "Async" 로 끝나면 비동기로 실행된다는 의미이다.
     * 별도의 executor를 지정하지 않으면 공용 ForkJoinPool의 스레드에서 실행된다.
     */
    @Test
    public void thenAcceptAsyncExample() {
        StringBuilder result = new StringBuilder();
        CompletableFuture<Void> cf = CompletableFuture.completedFuture("thenAcceptAsync message")
                .thenAcceptAsync(s -> result.append(s));

        cf.join(); // 비동기로 수행되므로, 완료될 때 까지 기다린다.

        assertThat(result.length()).isGreaterThan(0);
    }

    /**
     * completeExceptionally 로 종료하기
     */
    @Test
    public void completeExceptionallyExample() {
        CompletableFuture<String> cf = CompletableFuture.supplyAsync(() -> {
            randomSleep();
            return "message";
        });

        // cf에서 exception이 발생할 때에 임의의 처리를 하여 결과값을 반환할 수 있음
        CompletableFuture<String> exceptionHandler = cf.handle((s, throwable) -> {
            if (throwable != null) {
                return "비동기 작업 중 exception이 발생하였음";
            }
            return s; // exception이 발생하지 않았으면 "message"이 반환됨
        });

        // 비동기로 실행 중인 cf를 예외로 종료시킴
        cf.completeExceptionally(new RuntimeException("예외가 발생해서 종료된 것으로 함"));

        try {
            // cf는 completed exceptionally 되었으므로 join() 은 결과값을 가져오지 않고 CompletionException 을 throw 함
            cf.join();
            fail("여기에 도달하면 fail");
        } catch (CompletionException e) {
            assertThat(e.getCause().getMessage()).isEqualTo("예외가 발생해서 종료된 것으로 함");
        }

        assertThat(exceptionHandler.join()).isEqualTo("비동기 작업 중 exception이 발생하였음");
    }

    /**
     * 작업을 취소하기
     */
    @Test
    public void cancelExample() {
        CompletableFuture<String> cf = CompletableFuture.supplyAsync(() -> {
            randomSleep();
            return "message";
        });

        // 비동기로 실행 중인 cf를 취소
        assertThat(cf.cancel(true)).isTrue();

        assertThat(cf.isCancelled()).isTrue();
    }

    /**
     * 두 개의 작업 중 먼저 완료되는 작업의 결과에 Function 수행하기
     * Function은 먼저 완료되는 작업이 수행되었던 thread에서 수행된다(매우 빨리 끝났다면 main thread에서 수행될 수도 있다).
     */
    @Test
    public void applyToEitherException() {
        StringBuffer fastStepThreadName = new StringBuffer();
        StringBuffer slowStepThreadName = new StringBuffer();

        CompletableFuture<String> fastApiCf = CompletableFuture.supplyAsync(() -> {
            fastStepThreadName.append(getThreadName());
            fixedSleep(100);
            return "fast";
        });

        CompletableFuture<String> slowApiCf = CompletableFuture.supplyAsync(() -> {
            slowStepThreadName.append(getThreadName());
            fixedSleep(200);
            return "slow";
        });

        long start = System.currentTimeMillis();

        // applyToEither(other: CompletionStage<T>, function: Function<T, R>): CompletableFuture<R>
        CompletableFuture<String> finalCf = fastApiCf.applyToEither(slowApiCf, s -> {
            assertThat(isCommonForkJoinPoolThread()).isTrue();
            assertThat(getThreadName()).isEqualTo(fastStepThreadName.toString());
            return s;
        });

        assertThat(finalCf.join()).isEqualTo("fast");

        long elapsed = System.currentTimeMillis() - start;
        assertThat(elapsed).isGreaterThanOrEqualTo(100);
        assertThat(elapsed).isLessThan(200);
    }

    private String getThreadName() {
        return Thread.currentThread().getName();
    }

    private void logThread(String taskName) {
        log.info("{} - thread name: {}", taskName, getThreadName());
    }

    /**
     * 두 개의 작업 중 먼저 완료되는 작업의 결과에 Consumer 수행하기
     * Consumer는 먼저 완료되는 작업이 수행되었던 thread에서 수행된다(매우 빨리 끝났다면 main thread에서 수행될 수도 있다).
     */
    @Test
    public void acceptToEitherException() {
        StringBuffer fastStepThreadName = new StringBuffer();
        StringBuffer slowStepThreadName = new StringBuffer();

        CompletableFuture<String> fastApiCf = CompletableFuture.supplyAsync(() -> {
            fastStepThreadName.append(getThreadName());
            fixedSleep(100);
            return "fast";
        });

        CompletableFuture<String> slowApiCf = CompletableFuture.supplyAsync(() -> {
            slowStepThreadName.append(getThreadName());
            fixedSleep(200);
            return "slow";
        });

        // thread-safe한 StringBuffer를 사용하였음
        StringBuffer sb = new StringBuffer();

        long start = System.currentTimeMillis();

        // acceptEither(other: CompletionStage<T>, consumer: Consumer<T>): CompletableFuture<Void>
        CompletableFuture<Void> finalCf = fastApiCf.acceptEither(slowApiCf, s -> {
            assertThat(isCommonForkJoinPoolThread()).isTrue();
            assertThat(getThreadName()).isEqualTo(fastStepThreadName.toString());
            sb.append(s);
        });

        finalCf.join();
        assertThat(sb.toString()).isEqualTo("fast");

        long elapsed = System.currentTimeMillis() - start;
        assertThat(elapsed).isGreaterThanOrEqualTo(100);
        assertThat(elapsed).isLessThan(200);
    }

    /**
     * 두 작업이 모두 끝나면 Runnable을 수행하기
     * Consumer는 나중에 완료되는 작업이 수행되었던 thread에서 수행된다(매우 빨리 끝났다면 main thread에서 수행될 수도 있다).
     */
    @Test
    public void runAfterBothExample() {
        StringBuffer fastStepThreadName = new StringBuffer();
        StringBuffer slowStepThreadName = new StringBuffer();
        StringBuilder sb = new StringBuilder(); // for thread-safe

        CompletableFuture<Void> fastApiCf = CompletableFuture.runAsync(() -> {
            fastStepThreadName.append(getThreadName());
            fixedSleep(100);
            sb.append("Fast");
        });

        CompletableFuture<Void> slowApiCf = CompletableFuture.runAsync(() -> {
            slowStepThreadName.append(getThreadName());
            fixedSleep(200);
            sb.append("Slow");
        });

        long start = System.currentTimeMillis();

        CompletableFuture<Void> finalCf = fastApiCf.runAfterBoth(slowApiCf, () -> {
            assertThat(isCommonForkJoinPoolThread()).isTrue();
            assertThat(getThreadName()).isEqualTo(slowStepThreadName.toString());
            sb.append("Final");
        });
        finalCf.join();

        long elapsed = System.currentTimeMillis() - start;

        assertThat(sb.toString()).isEqualTo("FastSlowFinal");
        assertThat(elapsed).isGreaterThanOrEqualTo(200);
        assertThat(elapsed).isLessThan(300);
    }

    /**
     * 두 작업이 모두 끝나면 BiConsumer을 수행하기
     * BiConsumer는 나중에 완료되는 작업이 수행되었던 thread에서 수행된다(매우 빨리 끝났다면 main thread에서 수행될 수도 있다).
     */
    @Test
    public void acceptBothExample() {
        StringBuffer fastStepThreadName = new StringBuffer();
        StringBuffer slowStepThreadName = new StringBuffer();
        StringBuilder sb = new StringBuilder(); // for thread-safe

        CompletableFuture<String> fastApiCf = CompletableFuture.supplyAsync(() -> {
            fastStepThreadName.append(getThreadName());
            fixedSleep(100);
            return "Fast";
        });

        CompletableFuture<String> slowApiCf = CompletableFuture.supplyAsync(() -> {
            slowStepThreadName.append(getThreadName());
            fixedSleep(200);
            return "Slow";
        });

        long start = System.currentTimeMillis();

        CompletableFuture<Void> finalCf = fastApiCf.thenAcceptBoth(slowApiCf, (s1, s2) -> {
            assertThat(isCommonForkJoinPoolThread()).isTrue();
            assertThat(getThreadName()).isEqualTo(slowStepThreadName.toString());
            sb.append(s1).append(s2).append("Final");
        });
        finalCf.join();

        long elapsed = System.currentTimeMillis() - start;

        assertThat(sb.toString()).isEqualTo("FastSlowFinal");
        assertThat(elapsed).isGreaterThanOrEqualTo(200);
        assertThat(elapsed).isLessThan(300);
    }

    /**
     * 두 작업이 모두 끝나면 BiFunction을 수행하기
     * BiFunction은 나중에 완료되는 작업이 수행되었던 thread에서 수행된다(매우 빨리 끝났다면 main thread에서 수행될 수도 있다).
     */
    @Test
    public void thenCombineExample() {
        StringBuffer fastStepThreadName = new StringBuffer();
        StringBuffer slowStepThreadName = new StringBuffer();

        CompletableFuture<String> fastApiCf = CompletableFuture.supplyAsync(() -> {
            fastStepThreadName.append(getThreadName());
            fixedSleep(100);
            return "Fast";
        });

        CompletableFuture<String> slowApiCf = CompletableFuture.supplyAsync(() -> {
            slowStepThreadName.append(getThreadName());
            fixedSleep(200);
            return "Slow";
        });

        long start = System.currentTimeMillis();

        CompletableFuture<String> finalCf = fastApiCf.thenCombine(slowApiCf, (s1, s2) -> {
            assertThat(isCommonForkJoinPoolThread()).isTrue();
            assertThat(getThreadName()).isEqualTo(slowStepThreadName.toString());
            return s1 + s2 + "Final";
        });
        String result = finalCf.join();

        long elapsed = System.currentTimeMillis() - start;

        assertThat(result).isEqualTo("FastSlowFinal");
        assertThat(elapsed).isGreaterThanOrEqualTo(200);
        assertThat(elapsed).isLessThan(300);
    }

    /**
     * 두 개의 CompletableFuture를 순서대로 조합한다. 첫 번째 cf의 결과가 두 번째 cf의 입력으로 들어간다.
     * thenCompose는 두 개의 CompletableFuture를 연결하기 위한 것이고,
     * thenApply, thenAccept, thenRun은 CompletableFuture와 function/consumer/runnable 을 연결하기 위한 것이다.
     * Function은 첫 번째 cf가 수행되었던 thread에서 수행된다(매우 빨리 끝났다면 main thread에서 수행될 수도 있다).
     */
    @Test
    public void thenComposeExample() {
        StringBuffer previousStepThreadName = new StringBuffer();

        CompletableFuture<String> previousCf = CompletableFuture.supplyAsync(() -> {
            previousStepThreadName.append(getThreadName());
            randomSleep();
            return "Previous";
        });

        CompletableFuture<String> finalCf = previousCf.thenCompose(prevStepResult -> {
            assertThat(isCommonForkJoinPoolThread()).isTrue();
            assertThat(getThreadName()).isEqualTo(previousStepThreadName.toString());
            return CompletableFuture.completedFuture("Next").thenApply(nextResult -> prevStepResult + nextResult);
        });

        assertThat(finalCf.join()).isEqualTo("PreviousNext");
    }

    /**
     * 동일한 결과를 반환하는 여러 개의 작업 중 어느 하나가 완료되면 완료하기
     * 동일한 결과를 반환하는 작업이 있고, 그 중 가장 빠른 결과를 사용하고자 할 때 유용하다.
     * 처음으로 하나의 작업이 완료되면 즉시 whenComplete()로 제어가 넘어간다. 나머지 작업들은 버려진다.
     */
    @Test
    public void anyOfExample() {
        StringBuilder result = new StringBuilder();

        List<String> messages = Arrays.asList("a", "b", "c");

        CompletableFuture<String>[] completableFutures = messages.stream()
                .map(message -> CompletableFuture.supplyAsync(() -> {
                    randomSleep();
                    String upperCaseMessage = message.toUpperCase();
                    log.info("'{}' task completed -> '{}'", message, upperCaseMessage);
                    return upperCaseMessage;
                }))
                .toArray(CompletableFuture[]::new);

        // whenComplete()에 진입할 때는 하나의 feature가 완료된 시점이다.
        CompletableFuture<Object> cf = CompletableFuture.anyOf(completableFutures).whenComplete((firstResult, throwable) -> {
            log.info("firstResult: {}", firstResult);

            if (throwable == null) {
                assertThat((String) firstResult).isUpperCase();
                result.append(firstResult);
            }
        });

        cf.join();
        assertThat(result.length()).isEqualTo(1);
        log.info("result: {}", result);
    }

    /**
     * 여러 개의 작업 모두가 완료되면 완료하기
     * 서로 다른 처리를 하는 작업들을 병렬로 실행하여 그 결과들을 모두 사용하고자 할 때 유용하다.
     */
    @Test
    public void allOfExample() {
        StringBuilder allResult = new StringBuilder();

        List<String> messages = Arrays.asList("a", "b", "c");

        List<CompletableFuture<String>> futures = messages.stream()
                .map(message -> CompletableFuture.supplyAsync(() -> {
                    randomSleep();
                    String upperCaseMessage = message.toUpperCase();
                    log.info("'{}' task completed -> '{}'", message, upperCaseMessage);
                    return upperCaseMessage;
                })).collect(toList());

        CompletableFuture<String>[] completableFutures = futures.toArray(new CompletableFuture[futures.size()]);

        // whenComplete()에 진입할 때는 모든 features가 완료된 시점이다.
        // allOf(CompletableFuture<?> ...): CompletableFuture<Void> 이기 때문에 결과값은 null(Void)이다.
        // 그래서 개별 future로 부터 결과값을 얻어야 한다.
        CompletableFuture<Void> cf = CompletableFuture.allOf(completableFutures).whenComplete((voidValue, throwable) -> {
            assertThat(voidValue).isNull();

            if (throwable == null) {
                futures.forEach(future -> {
                    String result = future.getNow(null);
                    assertThat(result).isUpperCase();
                    allResult.append(result);
                });
            }
        });

        cf.join();
        assertThat(allResult.toString()).isEqualTo("ABC"); // features가 배열 순서와 일치하기 때문에 ABC
    }

    /**
     * CompletableFuture 의 실행시에 Executor 를 별도로 지정하지 않으면 JVM의 공용 ForkJoinPool 에서 비동기 작업이 수행된다.
     * 여기에 속해 있는 스레드는 daemon 스레드이다.
     */
    private boolean isCommonForkJoinPoolThread() {
        return Thread.currentThread().isDaemon();
    }

    private boolean isCustomExecutorThread() {
        return getThreadName().startsWith("custom-executor-");
    }

    private void randomSleep() {
        try { Thread.sleep(BASE_SLEEP_MS + RANDOM.nextInt(MAX_SLEEP_MS)); } catch (InterruptedException ignore) { }
    }

    private void sleepEnough() {
        try { Thread.sleep(BASE_SLEEP_MS + MAX_SLEEP_MS); } catch (InterruptedException ignore) { }
    }

    private void fixedSleep(int millis) {
        try { Thread.sleep(millis); } catch (InterruptedException ignore) { }
    }

}
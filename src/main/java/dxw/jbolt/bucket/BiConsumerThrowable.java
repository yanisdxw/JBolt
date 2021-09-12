package dxw.jbolt.bucket;

@FunctionalInterface
public interface BiConsumerThrowable<T, U> {
    void accept(T t, U u) throws Exception;
}

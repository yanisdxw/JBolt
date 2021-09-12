package dxw.jbolt.bucket;

@FunctionalInterface
public interface TriConsumerThrowable<T,U,S> {
    void accept(T t, U u, S s) throws Exception;
}

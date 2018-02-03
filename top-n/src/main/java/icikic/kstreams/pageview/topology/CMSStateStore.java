package icikic.kstreams.pageview.topology;

import com.twitter.algebird.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import scala.Function1;
import scala.math.BigDecimal;

// TODO
public class CMSStateStore implements StateStore {


    public CMSStateStore() {
        TopNCMSMonoid<Long> monoid = TopNCMS.monoid(10, 10, 10, 100, new JavaCMSHasherImpl());

    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    interface JavaCMSHasher<T> extends CMSHasher<T> {

        @Override
        int hash(int a, int b, int width, T x);

//        @Override
//        default <L> T on(Function1<L, T> f) {
//
//        }

//        @Override
//        default <L> Object contramap(Function1<L, Long> f) {
//
//        }
    }

    public static class JavaCMSHasherImpl implements CMSHasher<Long> {

        @Override
        public int hash(int a, int b, int width, Long x) {
            return 0;
        }
    }
}

package icikic.kstreams.pageview.producer;

import icikic.kstreams.pageview.domain.PageUpdate;
import icikic.kstreams.pageview.domain.PageView;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.readLines;
import static icikic.kstreams.pageview.producer.Generators.ResourceRandomizer.from;
import static java.util.Arrays.asList;

public class Generators {

    public static Stream<PageView> infinitePageViews() {
        return Stream.generate(new PageViewSupplier());
    }

    public static Stream <PageUpdate> finitePageUpdates() {
        return StreamSupport.stream(new PageUpdateIterable().spliterator(), false);
    }

    public static class PageUpdateIterable implements Iterable<PageUpdate> {
        private final Iterator<String> pagesIterator;
        private final Supplier<String> userSupplier;

        PageUpdateIterable() {
            this.userSupplier = from("/users.txt");
            this.pagesIterator = asList(from("/pages.txt").all()).iterator();
        }

        @Override
        public Iterator<PageUpdate> iterator() {
            return new Iterator<PageUpdate>() {
                @Override
                public boolean hasNext() {
                    return pagesIterator.hasNext();
                }

                @Override
                public PageUpdate next() {
                    return new PageUpdate(pagesIterator.next(), RandomStringUtils.randomAscii(100), userSupplier.get());
                }
            };
        }
    }

    public static class PageViewSupplier implements Supplier<PageView> {

        private final Supplier<String> userSupplier;
        private final Supplier<String> pageSupplier;
        private final Supplier<String> countrySupplier;

        PageViewSupplier() {
            this.userSupplier = from("/users.txt");
            this.pageSupplier = from("/pages.txt");
            this.countrySupplier = from("/countries.txt");
        }

        @Override
        public PageView get() {
            return new PageView(pageSupplier.get(), userSupplier.get(), countrySupplier.get());
        }
    }

    static class ResourceRandomizer implements Supplier<String> {
        private final String[] lines;
        private final Random random;

        static ResourceRandomizer from(String resource) {
            try {
                return new ResourceRandomizer(resource);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private ResourceRandomizer(String resource) throws IOException {
            final List<String> lines = readLines(getResource(resource), StandardCharsets.UTF_8);
            this.lines = lines.toArray(new String[lines.size()]);
            this.random = new Random();
        }

        public String[] all() {
            return Arrays.copyOf(lines, lines.length);
        }

        @Override
        public String get() {
            return lines[random.nextInt(lines.length)];
        }
    }
}

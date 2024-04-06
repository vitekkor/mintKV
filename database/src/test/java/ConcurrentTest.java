import com.mint.db.Entry;
import com.mint.db.impl.StringDaoWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import util.BaseTest;

import java.util.List;


class ConcurrentTest extends BaseTest {
    private final StringDaoWrapper dao = new StringDaoWrapper();

    @Test
    void test_10_000() throws Exception {
        int count = 10_000;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(4, count, value -> dao.upsert(entries.get(value))).close();
        assertSame(dao.all(), entries);
    }


    @Test
    @Timeout(15)
    void testConcurrentRW_2_500() throws Exception {
        int count = 2_500;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(4, count, value -> {
            dao.upsert(entries.get(value));
            assertContains(dao.all(), entries.get(value));
        }).close();

        assertSame(dao.all(), entries);
    }

    @Test
    void testConcurrentRead_8_000() throws Exception {
        int count = 8_000;
        List<Entry<String>> entries = entries("k", "v", count);
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }
        runInParallel(4, count, value -> assertContains(dao.all(), entries.get(value))).close();

        assertSame(dao.all(), entries);
    }

}
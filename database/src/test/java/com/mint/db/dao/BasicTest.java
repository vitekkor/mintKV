package com.mint.db.dao;

import com.mint.db.dao.impl.StringDaoWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import com.mint.db.dao.util.BaseTest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class BasicTest extends BaseTest {
    private final StringDaoWrapper dao = new StringDaoWrapper();

    @Test
    void testEmpty() {
        assertEmpty(dao.all());
    }

    @Test
    void testSingle() {
        dao.upsert(entry("a", "b"));
        assertSame(
                dao.all(),
                entry("a", "b")
        );
    }

    @Test
    void testOrder() {
        dao.upsert(entry("b", "b"));
        dao.upsert(entry("aa", "aa"));
        dao.upsert(entry("", ""));

        assertSame(
                dao.all(),

                entry("", ""),
                entry("aa", "aa"),
                entry("b", "b")
        );
    }

    @Test
    void testOrder2() {
        dao.upsert(entry("aa", "aa"));
        dao.upsert(entry("b", "b"));
        dao.upsert(entry("", ""));

        assertSame(
                dao.all(),

                entry("", ""),
                entry("aa", "aa"),
                entry("b", "b")
        );
    }

    @Test
    void testTree() {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        assertSame(
                dao.all(),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @Test
    void testManyIterators() {
        List<Entry<String>> entries = new ArrayList<>(entries(10_000));
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }
        try {
            List<Iterator<Entry<String>>> iterators = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                iterators.add(dao.all());
            }
            // just utilize the collection
            Assertions.assertEquals(10_000, iterators.size());
        } catch (OutOfMemoryError error) {
            throw new AssertionFailedError("Too much data in memory: use some lazy ways", error);
        }
    }

    @Test
    void testFindValueInTheMiddle() {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        assertSame(dao.get("c"), entry("c", "d"));
    }

    @Test
    void testHugeData() throws Exception {
        final int entries = 100_000;

        for (int entry = 0; entry < entries; entry++) {
            final int e = entry;

            // Retry if autoflush is too slow
            retry(() -> dao.upsert(entry(keyAt(e), valueAt(e))));
        }

        for (int entry = 0; entry < entries; entry++) {
            assertSame(dao.get(keyAt(entry)), entryAt(entry));
        }
    }
}
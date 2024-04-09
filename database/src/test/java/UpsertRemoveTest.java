import com.mint.db.impl.StringDaoWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import util.BaseTest;

class UpsertRemoveTest extends BaseTest {
    private final StringDaoWrapper dao = new StringDaoWrapper();

    @Test
    void removeShouldUpsertNullValue() {
        dao.upsert(entryAt(1));
        dao.upsert(entry(keyAt(1), null));
        Assertions.assertNull(dao.get(keyAt(1)).value());
    }

    @Test
    void removeShouldNotAffectOtherEntries() {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.upsert(entry(keyAt(1), null));
        Assertions.assertNotNull(dao.get(keyAt(2)).value());
    }

    @Test
    void removeTwiceShouldNotThrowException() {
        dao.upsert(entryAt(1));
        dao.upsert(entry(keyAt(1), null));
        Assertions.assertDoesNotThrow(() -> dao.upsert(entry(keyAt(1), null)));
    }
}

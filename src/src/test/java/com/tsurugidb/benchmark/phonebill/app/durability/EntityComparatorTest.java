package com.tsurugidb.benchmark.phonebill.app.durability;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;

import org.junit.jupiter.api.Test;
class EntityComparatorTest {


    @Test
    void testExpectedOnly() {
        List<DummyEntity> expected = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("2", "B")
        );
        List<DummyEntity> actual = Arrays.asList(
            new DummyEntity("1", "A")
        );
        EntityComparator<DummyEntity, String> comparator
            = new EntityComparator<>(expected, actual, DummyEntity::getKey, "DummyEntity");
        assertEquals(1, comparator.getExpectedOnly().size());
        assertEquals("2", comparator.getExpectedOnly().get(0).getKey());
        assertFalse(comparator.areCollectionsExactlyEqual());
        String expectedString ="DummyEntity present only expected:\n"+
                "DummyEntity [key=2, value=B]\n"+
                "\n"+
                "DummyEntity present only actual:\n"+
                "\n"+
                "DummyEntity that have changed:\n";
        assertEquals(expectedString, comparator.getComparisonResultAsString());
    }

    @Test
    void testActualOnly() {
        List<DummyEntity> expected = Arrays.asList(
            new DummyEntity("1", "A")
        );
        List<DummyEntity> actual = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("3", "C")
        );
        EntityComparator<DummyEntity, String> comparator
            = new EntityComparator<>(expected, actual, DummyEntity::getKey, "DummyEntity");
        assertEquals(1, comparator.getActualOnly().size());
        assertEquals("3", comparator.getActualOnly().get(0).getKey());
        assertFalse(comparator.areCollectionsExactlyEqual());
        String expectedString ="DummyEntity present only expected:\n"+
                "\n"+
                "DummyEntity present only actual:\n"+
                "DummyEntity [key=3, value=C]\n"+
                "\n"+
                "DummyEntity that have changed:\n";
        assertEquals(expectedString, comparator.getComparisonResultAsString());
    }

    @Test
    void testSameInBoth() {
        List<DummyEntity> expected = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("2", "B")
        );
        List<DummyEntity> actual = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("2", "B")
        );
        EntityComparator<DummyEntity, String> comparator
            = new EntityComparator<>(expected, actual, DummyEntity::getKey, "DummyEntity");
        assertEquals(2, comparator.getSameInBoth().size());
        assertTrue(comparator.areCollectionsExactlyEqual());
        String expectedString ="DummyEntity present only expected:\n"+
                "\n"+
                "DummyEntity present only actual:\n"+
                "\n"+
                "DummyEntity that have changed:\n";
        assertEquals(expectedString, comparator.getComparisonResultAsString());
    }

    @Test
    void testDifferentInBoth() {
        List<DummyEntity> expected = Arrays.asList(
            new DummyEntity("1", "A")
        );
        List<DummyEntity> actual = Arrays.asList(
            new DummyEntity("1", "B")
        );
        EntityComparator<DummyEntity, String> comparator
            = new EntityComparator<>(expected, actual, DummyEntity::getKey, "DummyEntity");
        assertEquals(1, comparator.getDifferentInBoth().size());
        assertEquals("A", comparator.getDifferentInBoth().get(0).getKey().getValue());
        assertEquals("B", comparator.getDifferentInBoth().get(0).getValue().getValue());
        assertFalse(comparator.areCollectionsExactlyEqual());
        String expectedString ="DummyEntity present only expected:\n"+
                "\n"+
                "DummyEntity present only actual:\n"+
                "\n"+
                "DummyEntity that have changed:\n"+
                "expected: DummyEntity [key=1, value=A]\n"+
                "actual: DummyEntity [key=1, value=B]\n\n";
        assertEquals(expectedString, comparator.getComparisonResultAsString());
    }

    @Test
    void testCustomEquality() {
        List<DummyEntity> expected = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("2", "B")
        );
        List<DummyEntity> actual = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("2", "C")
        );
        // すべてのフィールドが等しい場合にのみ等しいと判断するカスタム等価性ロジック
        BiPredicate<DummyEntity, DummyEntity> customEquality = (entity1, entity2) ->
            entity1.getKey().equals(entity2.getKey()) && entity1.getValue().equals(entity2.getValue());

        EntityComparator<DummyEntity, String> comparator
            = new EntityComparator<>(expected, actual, DummyEntity::getKey, customEquality, "DummyEntity");

        assertEquals(0, comparator.getExpectedOnly().size());
        assertEquals(0, comparator.getActualOnly().size());
        assertEquals(1, comparator.getSameInBoth().size());
        assertEquals("1", comparator.getSameInBoth().get(0).getKey());
        assertEquals(1, comparator.getDifferentInBoth().size());
        assertEquals("2", comparator.getDifferentInBoth().get(0).getKey().getKey());
        assertFalse(comparator.areCollectionsExactlyEqual());
    }

    @Test
    void testCustomEqualityKeyOnly() {
        List<DummyEntity> expected = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("2", "B")
        );
        List<DummyEntity> actual = Arrays.asList(
            new DummyEntity("1", "A"),
            new DummyEntity("2", "C")
        );
        // キーが等しい場合に等しいと判断するカスタム等価性ロジック
        BiPredicate<DummyEntity, DummyEntity> keyOnlyEquality = (entity1, entity2) ->
            entity1.getKey().equals(entity2.getKey());

        EntityComparator<DummyEntity, String> comparator
            = new EntityComparator<>(expected, actual, DummyEntity::getKey, keyOnlyEquality, "DummyEntity");

        assertEquals(0, comparator.getExpectedOnly().size());
        assertEquals(0, comparator.getActualOnly().size());
        assertEquals(2, comparator.getSameInBoth().size());
        assertTrue(comparator.areCollectionsExactlyEqual());
    }

    public static class DummyEntity {
        private String key;
        private String value;

        public DummyEntity(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DummyEntity other = (DummyEntity) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("DummyEntity [key=");
            builder.append(key);
            builder.append(", value=");
            builder.append(value);
            builder.append("]");
            return builder.toString();
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }

}

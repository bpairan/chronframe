package com.bpairan.chronframe;

import com.bpairan.chronframe.converter.ToIntConverter;
import com.bpairan.chronframe.converter.ToStringConverter;
import com.bpairan.chronframe.converter.ValueConverter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.Option;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by Bharathi Pairan on 24/09/2022.
 */
public class ChronFrameTest {

    @Test
    public void shouldReturnLength() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            assertEquals(2, cf.length());
        }
    }

    @Test
    public void shouldGetFirstDocument() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            ChronFrameRow row0 = cf.apply(0);
            String name0 = row0.valueOf("name", ValueConverter.toStringConverter()).get();
            assertEquals("Emily", name0);
            assertEquals(33, row0.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(169, row0.valueOf("height", ValueConverter.toIntConverter()).get());
            assertEquals("London", row0.valueOf("city", ValueConverter.toStringConverter()).get());
        }
    }

    @Test
    public void shouldReadNull() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            ChronFrameRow row0 = cf.apply(1);
            assertEquals("Thomas", row0.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(25, row0.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(Option.empty(), row0.valueOf("height", ValueConverter.toIntConverter()));
            assertEquals(Option.empty(), row0.valueOf("city", ValueConverter.toStringConverter()));
        }
    }

    @Test
    public void shouldThrowException() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            IllegalArgumentException thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> cf.apply(2));
            assertEquals("index: 2 not found", thrown.getMessage());

        }
    }

    @Test
    public void shouldRenameExistingColumn() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            ChronFrame cfRenamed = cf.rename("city", "place");
            ChronFrameRow row = cfRenamed.apply(0);
            assertEquals("London", row.valueOf("place", ValueConverter.toStringConverter()).get());
        }
    }

    @Test
    public void shouldRenameMultipleColumn() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            Map<String, String> renameMap = Maps.newHashMap();
            renameMap.put("height", "height_cm");
            renameMap.put("city", "place");
            ChronFrame cfRenamed = cf.rename(renameMap);
            ChronFrameRow row = cfRenamed.apply(0);
            assertEquals(169, row.valueOf("height_cm", ValueConverter.toIntConverter()).get());
            assertEquals("London", row.valueOf("place", ValueConverter.toStringConverter()).get());
        }
    }

    @Test
    public void shouldDropColumnsFromFrame() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            List<String> dropList = Lists.newArrayList("height", "city");
            ChronFrame cfDropped = cf.drop(dropList);

            ChronFrameRow row1 = cfDropped.apply(0);
            assertEquals("Emily", row1.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(33, row1.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(Option.empty(), row1.valueOf("height", ValueConverter.toStringConverter()));
            assertEquals(Option.empty(), row1.valueOf("city", ValueConverter.toStringConverter()));

            ChronFrameRow row2 = cfDropped.apply(1);
            assertEquals("Thomas", row2.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(25, row2.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(Option.empty(), row2.valueOf("height", ValueConverter.toStringConverter()));
            assertEquals(Option.empty(), row2.valueOf("city", ValueConverter.toStringConverter()));
        }
    }

    @Test
    public void shouldAddRow() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            Map<String, Object> newRow = Maps.newHashMap();
            newRow.put("name", "X");
            newRow.put("age", 43);
            newRow.put("height", 180);
            newRow.put("city", "Yard");
            ChronFrame cfAdded = cf.addRow(newRow);

            ChronFrameRow row1 = cfAdded.apply(0);
            assertEquals("Emily", row1.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(33, row1.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(169, row1.valueOf("height", ValueConverter.toIntConverter()).get());
            assertEquals("London", row1.valueOf("city", ValueConverter.toStringConverter()).get());

            ChronFrameRow row2 = cfAdded.apply(1);
            assertEquals("Thomas", row2.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(25, row2.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(Option.empty(), row2.valueOf("height", ValueConverter.toIntConverter()));
            assertEquals(Option.empty(), row2.valueOf("city", ValueConverter.toStringConverter()));

            ChronFrameRow row3 = cfAdded.apply(2);
            assertEquals("X", row3.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(43, row3.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(180, row3.valueOf("height", ValueConverter.toIntConverter()).get());
            assertEquals("Yard", row3.valueOf("city", ValueConverter.toStringConverter()).get());
        }
    }

    @Test
    public void shouldAddAllRows() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            Map<String, Object> newRow1 = Maps.newHashMap();
            newRow1.put("name", "X");
            newRow1.put("age", 43);
            newRow1.put("height", 180);
            newRow1.put("city", "Yard");
            Map<String, Object> newRow2 = Maps.newHashMap();
            newRow2.put("name", "Y");
            newRow2.put("age", 13);
            newRow2.put("height", 160);
            newRow2.put("city", "Boat");

            List<Map<String, Object>> data = Lists.newArrayList(newRow1, newRow2);

            ChronFrame cfAdded = cf.addRows(data);

            ChronFrameRow row1 = cfAdded.apply(0);
            assertEquals("Emily", row1.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(33, row1.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(169, row1.valueOf("height", ValueConverter.toIntConverter()).get());
            assertEquals("London", row1.valueOf("city", ValueConverter.toStringConverter()).get());

            ChronFrameRow row2 = cfAdded.apply(1);
            assertEquals("Thomas", row2.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(25, row2.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(Option.empty(), row2.valueOf("height", ValueConverter.toIntConverter()));
            assertEquals(Option.empty(), row2.valueOf("city", ValueConverter.toStringConverter()));

            ChronFrameRow row3 = cfAdded.apply(2);
            assertEquals("X", row3.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(43, row3.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(180, row3.valueOf("height", ValueConverter.toIntConverter()).get());
            assertEquals("Yard", row3.valueOf("city", ValueConverter.toStringConverter()).get());

            ChronFrameRow row4 = cfAdded.apply(3);
            assertEquals("Y", row4.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(13, row4.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(160, row4.valueOf("height", ValueConverter.toIntConverter()).get());
            assertEquals("Boat", row4.valueOf("city", ValueConverter.toStringConverter()).get());
        }
    }

    @Test
    public void shouldThrowExceptionWhenAppendColumnWithWrongSize() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            IllegalArgumentException thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> cf.appendColumn("country", Lists.newArrayList("UK")));
            assertEquals("size of data should be same as size of ChronFrame", thrown.getMessage());
        }
    }

    @Test
    public void shouldAppendNewColumn() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        List<Object> newColumns = Lists.newArrayList("UK", "France");
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().map(c -> c.appendColumn("country", newColumns)).get()) {

            ChronFrameRow row1 = cf.apply(0);
            assertEquals("Emily", row1.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(33, row1.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(169, row1.valueOf("height", ValueConverter.toIntConverter()).get());
            assertEquals("London", row1.valueOf("city", ValueConverter.toStringConverter()).get());
            assertEquals("UK", row1.valueOf("country", ValueConverter.toStringConverter()).get());

            ChronFrameRow row2 = cf.apply(1);
            assertEquals("Thomas", row2.valueOf("name", ValueConverter.toStringConverter()).get());
            assertEquals(25, row2.valueOf("age", ValueConverter.toIntConverter()).get());
            assertEquals(Option.empty(), row2.valueOf("height", ValueConverter.toIntConverter()));
            assertEquals(Option.empty(), row2.valueOf("city", ValueConverter.toStringConverter()));
            assertEquals("France", row2.valueOf("country", ValueConverter.toStringConverter()).get());
        }
    }

    @Test
    public void shouldReturnAllColumns() {
        Path inputPath = TestCase.instance().testResource("sample.csv");
        ChronicleQueue queue = ChronFrame.instance().newChronicleQueue("test", Option.empty());
        try (ChronFrame cf = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption().get()) {
            assertEquals(Lists.newArrayList("name", "age", "height", "city"), cf.listOfColumns());
        }
    }
}

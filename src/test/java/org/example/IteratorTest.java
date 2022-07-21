package org.example;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IteratorTest {

    @Test
    public void iteratorTest() {

        ProjectOperator projectOperator = new ProjectOperator();
        ScanOperator scanOperator = new ScanOperator();
        FilterOperator filterOperator = new FilterOperator("id", 1);
        filterOperator.child = scanOperator;
        projectOperator.child = filterOperator;
        projectOperator.open();

        Row row = projectOperator.next();
        while (row != null) {
            System.out.println(row.print());
            row = projectOperator.next();
        }

    }
}


interface Iterator {
    void open();

    void close();

    Row next();
}

class Row {
    final Map<String, Object> data = new HashMap<>();

    public static Row dummy(String key, Object value, String key1, Object value1) {
        Row row = new Row();
        row.data.put(key, value);
        row.data.put(key1, value1);
        return row;
    }

    public String print() {
        return JsonUtils.asJson(this);
    }
}

class ScanOperator implements Iterator {
    private List<Row> items;
    private int index;

    @Override
    public void open() {
        items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            items.add(Row.dummy("id", i, "name", "harish_" + i));
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Row next() {
        if (index >= items.size()) {
            return null;
        }
        Row row = items.get(index);
        index++;
        return row;
    }
}

class ProjectOperator implements Iterator {
    Iterator child;

    @Override
    public void open() {
        child.open();
    }

    @Override
    public void close() {
    }

    @Override
    public Row next() {
        return child.next();
    }
}

class FilterOperator implements Iterator {
    final String key;
    final Object value;
    Iterator child;

    FilterOperator(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public void close() {
    }

    @Override
    public Row next() {
        Row toRet = child.next();
        while (toRet != null) {
            if (Objects.equals(toRet.data.get(key), value)) {
                return toRet;
            }
            toRet = child.next();
        }
        return null;
    }
}

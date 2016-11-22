package com.epam.bigdata.spark.model;

import java.io.Serializable;

/**
 * Created by Aliaksei_Neuski on 10/6/16.
 */
public class EventAttendsEntity implements Serializable {

    private String id;
    private String name;
    private int count;

    public EventAttendsEntity() {
    }

    public EventAttendsEntity(String id, String name, int count) {
        this.id = id;
        this.name = name;
        this.count = count;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventAttendsEntity that = (EventAttendsEntity) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "EventAttendsEntity{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", count=" + count +
                '}';
    }
}

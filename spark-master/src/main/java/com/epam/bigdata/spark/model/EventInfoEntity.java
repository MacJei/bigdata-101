package com.epam.bigdata.spark.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 10/5/16.
 */
public class EventInfoEntity implements Serializable {

    private String id;
    private String name;
    private String desc;
    private int attendingCount;
    private String city;
    private String date;
    private String tag;
    private List<EventAttendsEntity> allAttends = new ArrayList<>();


    public EventInfoEntity() {
    }

    public EventInfoEntity(String id, String name, String desc, int attendingCount, String city, String date, String tag) {
        this.id = id;
        this.name = name;
        this.desc = desc;
        this.attendingCount = attendingCount;
        this.city = city;
        this.date = date;
        this.tag = tag;
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

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getAttendingCount() {
        return attendingCount;
    }

    public void setAttendingCount(int attendingCount) {
        this.attendingCount = attendingCount;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public List<EventAttendsEntity> getAllAttends() {
        return allAttends;
    }

    public void setAllAttends(List<EventAttendsEntity> allAttends) {
        this.allAttends = allAttends;
    }

    public void addToAllAttends(EventAttendsEntity attend){
        allAttends.add(attend);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventInfoEntity that = (EventInfoEntity) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "EventInfoEntity{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", attendingCount=" + attendingCount +
                ", city='" + city + '\'' +
                ", date='" + date + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }
}

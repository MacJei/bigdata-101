package com.epam.bigdata.spark.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 10/5/16.
 */
public class EventsTagEntity implements Serializable {

    private String tag;
    private List<EventInfoEntity> allEvents;

    public EventsTagEntity() {
    }

    public EventsTagEntity(String tag, List<EventInfoEntity> allEvents) {
        this.tag = tag;
        this.allEvents = allEvents;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public List<EventInfoEntity> getAllEvents() {
        return allEvents;
    }

    public void setAllEvents(List<EventInfoEntity> allEvents) {
        this.allEvents = allEvents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventsTagEntity that = (EventsTagEntity) o;

        if (tag != null ? !tag.equals(that.tag) : that.tag != null) return false;
        return allEvents != null ? allEvents.equals(that.allEvents) : that.allEvents == null;

    }

    @Override
    public int hashCode() {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + (allEvents != null ? allEvents.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EventsTagEntity{" +
                "tag='" + tag + '\'' +
                ", allEvents=" + allEvents +
                '}';
    }
}

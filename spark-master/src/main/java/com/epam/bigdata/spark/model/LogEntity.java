package com.epam.bigdata.spark.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 10/4/16.
 */
public class LogEntity implements Serializable {

    private String date;

    private int cityId;
    private String city;

    private long userTagsId;
    private List<String> tags;

    public LogEntity() {
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public long getUserTagsId() {
        return userTagsId;
    }

    public void setUserTagsId(long userTagsId) {
        this.userTagsId = userTagsId;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogEntity logEntity = (LogEntity) o;

        if (cityId != logEntity.cityId) return false;
        if (userTagsId != logEntity.userTagsId) return false;
        return date != null ? date.equals(logEntity.date) : logEntity.date == null;

    }

    @Override
    public int hashCode() {
        int result = date != null ? date.hashCode() : 0;
        result = 31 * result + cityId;
        result = 31 * result + (int) (userTagsId ^ (userTagsId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LogEntity{" +
                "date='" + date + '\'' +
                ", cityId=" + cityId +
                ", city='" + city + '\'' +
                ", userTagsId=" + userTagsId +
                ", tags=" + tags +
                '}';
    }
}

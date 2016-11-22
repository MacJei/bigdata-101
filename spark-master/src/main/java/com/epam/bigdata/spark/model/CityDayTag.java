package com.epam.bigdata.spark.model;

import java.io.Serializable;

/**
 * Created by Aliaksei_Neuski on 10/6/16.
 */
public class CityDayTag implements Serializable {

    private String city;
    private String date;
    private String tag;

    public CityDayTag() {
    }

    public CityDayTag(String city, String date, String tag) {
        this.city = city;
        this.date = date;
        this.tag = tag;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CityDayTag that = (CityDayTag) o;

        if (city != null ? !city.equals(that.city) : that.city != null) return false;
        if (date != null ? !date.equals(that.date) : that.date != null) return false;
        return tag != null ? tag.equals(that.tag) : that.tag == null;

    }

    @Override
    public int hashCode() {
        int result = city != null ? city.hashCode() : 0;
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CityDayTag{" +
                "city='" + city + '\'' +
                ", date='" + date + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }
}

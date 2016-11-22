package com.epam.bigdata.spark.model;

import java.io.Serializable;

/**
 * Created by Aliaksei_Neuski on 10/5/16.
 */
public class CityDayPair implements Serializable {

    private String date;
    private String city;

    public CityDayPair() {
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CityDayPair that = (CityDayPair) o;

        if (date != null ? !date.equals(that.date) : that.date != null) return false;
        return city != null ? city.equals(that.city) : that.city == null;

    }

    @Override
    public int hashCode() {
        int result = date != null ? date.hashCode() : 0;
        result = 31 * result + (city != null ? city.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CityDayPair{" +
                "date='" + date + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
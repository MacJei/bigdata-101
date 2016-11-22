package com.epam.bigdata.task4.mr;

/**
 * Created by Aliaksei_Neuski on 9/7/16.
 */
public enum Insights {
    SITE_IMPRESSION("1")
    ;

    private final String text;

    private Insights(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
};
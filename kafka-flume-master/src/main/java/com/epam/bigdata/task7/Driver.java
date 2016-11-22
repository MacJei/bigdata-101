package com.epam.bigdata.task7;

import java.io.IOException;

/**
 * Created by Aliaksei_Neuski on 10/7/16.
 */
public class Driver {

    public static void main(String[] args) throws IOException {
        final String usage = "Usage: Driver <producer|consumer> <topic> <datapath>";

        if (args.length < 3) {
            throw new IllegalArgumentException(usage);
        }

        switch (args[0]) {
            case "producer":
                Producer.main(args);
                break;
            case "consumer":
                Consumer.main(args);
                break;
            default:
                throw new IllegalArgumentException(usage);
        }
    }
}

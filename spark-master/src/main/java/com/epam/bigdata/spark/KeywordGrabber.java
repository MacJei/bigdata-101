package com.epam.bigdata.spark;

import com.epam.bigdata.spark.model.*;
import com.restfb.*;
import com.restfb.types.Event;
import com.restfb.types.Location;
import com.restfb.types.Place;
import com.restfb.types.User;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

import com.restfb.json.JsonObject;
import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Created by Aliaksei_Neuski on 10/3/16.
 */
public class KeywordGrabber {

    /*
    * **********************************************************
    * FACEBOOK API CLIENT
    *
    * https://developers.facebook.com/docs/facebook-login/access-tokens/expiration-and-extension
    * **********************************************************
    */

    private static String MACHINE_TOKEN;
    private static FacebookClient FB_CLIENT;

    private static final List<String> stopWords = Arrays.asList("a", "and", "for", "to", "the", "you", "in");

    static {
        String verificationCode = null;
        try {
            verificationCode = new Scanner(new URL(FacebookGrabber.FB_CLIENT_TOKEN_API_CALL).openStream(), "UTF-8").
                    useDelimiter("\\A").next().replace("{\"code\":\"", "").replace("\"}", "");
        } catch (IOException e) {
            System.err.println(e.getLocalizedMessage());
        }

        try {
            MACHINE_TOKEN = new Scanner(new URL("https://graph.facebook.com/oauth/access_token?code="
                    + verificationCode + "&client_id=" + FacebookGrabber.CLIEN_ID + "&redirect_uri=")
                    .openStream(), "UTF-8")
                    .useDelimiter("\\A")
                    .next()
                    .replace("{\"access_token\":\"", "")
                    .split("\"")[0];
        } catch (IOException e) {
            System.err.println(e.getLocalizedMessage());
        }

        FB_CLIENT = new DefaultFacebookClient(MACHINE_TOKEN, Version.VERSION_2_7);
    }

    /*
    * **********************************************************
    * START
    * **********************************************************
    */

    /**
     *
     * @param tblName Hive ORC Table wirh dataset
     * @param tagsFilePath HDFS file with tags info
     * @param cityFilePath HDFS file with cities maps
     */
    public static void grab(String tblName, String tagsFilePath, String cityFilePath, boolean isUseFullDataset) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark HW-8")
                .config("spark.sql.warehouse.dir", "hdfs:///apps/hive/warehouse")
                .config("spark.sql.orc.filterPushdown", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.classesToRegister",
                        "com.epam.bigdata.spark.model.CityDayPair," +
                                "com.epam.bigdata.spark.model.LogEntity," +
                                "com.epam.bigdata.spark.model.EventInfoEntity," +
                                "com.epam.bigdata.spark.model.EventsTagEntity," +
                                "com.epam.bigdata.spark.model.CityDayTag," +
                                "com.epam.bigdata.spark.model.EventAttendsEntity"
                )
                .enableHiveSupport()
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        /*
        * **********************************************************
        * Part 1
        * **********************************************************
        */

        // show data test
        spark.sql("SELECT * FROM " + tblName + " LIMIT 10").show();

        Dataset<String> data;

        // tags
        data = spark.read().textFile(tagsFilePath);
        final String tagsHeader = data.first();

        JavaRDD<String> tagsRdd = data.filter(x -> !x.equals(tagsHeader)).javaRDD();
        JavaPairRDD<Long, List<String>> tagsIdsPairs = tagsRdd.mapToPair((PairFunction<String, Long, List<String>>) line -> {
            String[] parts = line.split("\\t");
            return new Tuple2<>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
        });
        final Map<Long, List<String>> tagsMap = tagsIdsPairs.collectAsMap();

        // cities
        data = spark.read().textFile(cityFilePath);
        final String citiesHeader = data.first();

        JavaRDD<String> citiesRdd = data.filter(x -> !x.equals(citiesHeader)).javaRDD();
        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRdd.mapToPair((PairFunction<String, Integer, String>) line -> {
            String[] parts = line.split("\\t");
            return new Tuple2<>(Integer.parseInt(parts[0]), parts[1]);
        });
        final Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();

        // dataset
        Dataset<Row> dataset = spark.read().format("orc").table(tblName);
        Dataset<Row> df = dataset.select("timestamp_date", "city", "user_tags");
        df.show();

        // logs: day city tags
        JavaRDD<LogEntity> logsRdd = df.toJavaRDD().map((Function<Row, LogEntity>) row -> {
            LogEntity logEntity = new LogEntity();

            String date = null != row.getString(0) ? row.getString(0) : "";
            date = date.length() >= 8 ? date.substring(0, 8) : "";
            logEntity.setDate(date);

            String city = Optional.ofNullable(citiesMap.get(row.getInt(1))).orElse("UNKNOWN");
            logEntity.setCity(city);

            List<String> tagsList = Optional.ofNullable(tagsMap.get(row.getLong(2))).orElse(Collections.emptyList());
            logEntity.setTags(tagsList);

            return logEntity;
        });

        // map-reduce
        JavaPairRDD<CityDayPair, Set<String>> cityDayPairs = logsRdd.mapToPair((PairFunction<LogEntity, CityDayPair, Set<String>>) logEntity -> {
            CityDayPair pair = new CityDayPair();
            pair.setCity(logEntity.getCity());
            pair.setDate(logEntity.getDate());
            return new Tuple2<>(pair, new HashSet<>(logEntity.getTags()));
        });
        JavaPairRDD<CityDayPair, Set<String>> cityDayWithTags = cityDayPairs.reduceByKey((Function2<Set<String>, Set<String>, Set<String>>) (e1, e2) -> {
            e1.addAll(e2);
            return e1;
        });

        // print
        List<Tuple2<CityDayPair, Set<String>>> result = cityDayWithTags.collect();
        for (Tuple2<CityDayPair, Set<String>> tuple : result) {
            System.out.println("DAY|" + tuple._1().getDate() + "|CITY|" + tuple._1().getCity() + "|TAGS|" + tuple._2() + "\n");
        }

        /*
        * **********************************************************
        * Part 2
        * **********************************************************
        */

        JavaRDD<String> uniqueTags;
        if (isUseFullDataset) {
            uniqueTags = logsRdd.flatMap(logsEntity -> logsEntity.getTags().iterator()).distinct();
        }
        else {
            List<String> tags = logsRdd.first().getTags();
            int size = tags.size();
            if (size > 10) {
                uniqueTags = sc.parallelize(tags.subList(0, 10));
            }
            else {
                uniqueTags = sc.parallelize(tags.subList(0, size));
            }
        }

        JavaRDD<EventsTagEntity> allEventsTagEntity = uniqueTags
                .map(tag -> {
                    Connection<Event> eventConnection = FB_CLIENT.fetchConnection(
                            "search", Event.class,
                            Parameter.with("q", tag),
                            Parameter.with("type", "event"),
                            Parameter.with("fields", FacebookGrabber.FB_EVENT_FIELDS));

                    final List<EventInfoEntity> eventInfoEntities = new ArrayList<>();

                    eventConnection.forEach(events -> events
                            .forEach(event -> {
                                if (event != null) {
                                    EventInfoEntity eventInfoEntity = new EventInfoEntity(event.getId(), event.getName(), event.getDescription(), event.getAttendingCount(), "", "", tag);
                                    String city = Optional.of(event)
                                            .map(Event::getPlace)
                                            .map(Place::getLocation)
                                            .map(Location::getCity)
                                            .orElse("UNKNOWN");
                                    eventInfoEntity.setCity(city);
                                    if (event.getStartTime() != null) {
                                        eventInfoEntity.setDate(FacebookGrabber.DATE_FORMAT.format(event.getStartTime()));
                                    }
                                    else {
                                        eventInfoEntity.setDate(FacebookGrabber.DEFAULT_DATE);
                                    }
                                    eventInfoEntities.add(eventInfoEntity);
                                }
                            }));
                    return new EventsTagEntity(tag, eventInfoEntities);
                });

        allEventsTagEntity.collect().forEach(tagEntity -> {
            System.out.println("#########################");
            System.out.println("#########################");

            tagEntity.getAllEvents().forEach(e ->
                    System.out.println(
                            ""
                                    + "|TAG|" + tagEntity.getTag()
                                    + "|DATE|" + e.getDate()
                                    + "|CITY|" + e.getCity()
                                    + "|ATTENDS|" + e.getAttendingCount()
                                    + "|ID|" + e.getId()
                                    + "|NAME|"  + e.getName()
                    )
            );
        });

        JavaRDD<EventInfoEntity> allEvents;
        if (isUseFullDataset) {
            allEvents = allEventsTagEntity.flatMap(tagEvent -> tagEvent.getAllEvents().iterator());
        }
        else {
            List<EventInfoEntity> infos = new ArrayList<>();
            for (EventsTagEntity e : allEventsTagEntity.take(2)) {
                infos.addAll(e.getAllEvents());
            }
            allEvents = sc.parallelize(infos);
            // allEvents = sc.parallelize(allEventsTagEntity.first().getAllEvents());
        }

        JavaPairRDD<CityDayTag, EventInfoEntity> cityDateTagSameKeys = allEvents.mapToPair(event -> {
            CityDayTag tagCityDateEntity = new CityDayTag(event.getCity(), event.getDate(), event.getTag());
            return new Tuple2<>(tagCityDateEntity, event);
        });

        JavaPairRDD<CityDayTag, EventInfoEntity> tagCityDatePairs = cityDateTagSameKeys.reduceByKey((event1, event2) -> {
            EventInfoEntity eventInfoEntity = new EventInfoEntity();
            eventInfoEntity.setAttendingCount(event1.getAttendingCount() + event2.getAttendingCount());
            eventInfoEntity.setDesc(event1.getDesc() + " " + event2.getDesc());
            return eventInfoEntity;
        });

        tagCityDatePairs.collect().forEach(tuple -> {
            if (tuple._1.getCity().equals("UNKNOWN")) {
                System.out.print(
                        ""
                                + "|TAG|" + tuple._1.getTag()
                                + "|CITY|" + tuple._1.getCity()
                                + "|DATE|" + tuple._1.getDate()
                                + "|ATTENDS|" + tuple._2.getAttendingCount()
                                + "|TOKEN_MAP|"
                );

                if (null != tuple._2.getDesc()) {
                    List<String> words = Pattern.compile("\\W").splitAsStream(tuple._2.getDesc())
                            .filter((s -> !s.isEmpty()))
                            .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
                            .filter(w -> !stopWords.contains(w))
                            .collect(toList());

                    words.stream()
                            .map(String::toLowerCase)
                            .collect(groupingBy(java.util.function.Function.identity(), counting()))
                            .entrySet().stream()
                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                            .limit(10).forEachOrdered(s -> System.out.print(s.getKey() + ", " + s.getValue() + ", "));
                    System.out.print(")");
                }

                System.out.println();
            }
        });

        /*
        * **********************************************************
        * Part 3
        * **********************************************************
        */

        JavaRDD<EventInfoEntity> allEventsWithAttends = allEvents.map(event -> {
            Connection<User> userConnections = FB_CLIENT.fetchConnection(event.getId() + "/attending", User.class, Parameter.with("limit", 100));
            EventInfoEntity eventInfoEntity = new EventInfoEntity();
            userConnections.forEach(users -> users
                    .forEach(user -> {
                        if (null != user) {
                            EventAttendsEntity eventAttendsEntity = new EventAttendsEntity(user.getId(), user.getName(), 0);
                            eventInfoEntity.addToAllAttends(eventAttendsEntity);
                        }
                    }));
            return eventInfoEntity;
        });

        JavaRDD<EventAttendsEntity> allAttends = allEventsWithAttends.flatMap(event -> event.getAllAttends().iterator());
        JavaPairRDD<EventAttendsEntity, Integer> allAttendsWithCount = allAttends.mapToPair(attend -> new Tuple2<>(attend, 1));
        JavaPairRDD<EventAttendsEntity, Integer> allAttendsWithSameName = allAttendsWithCount.reduceByKey((e1, e2) -> e1 + e2);
        JavaRDD<EventAttendsEntity> attendInformation =
                allAttendsWithSameName.map(t -> {
                    t._1.setCount(t._2);
                    return t._1;
                });

        JavaRDD<EventAttendsEntity> attendsLimit = attendInformation.sortBy(EventAttendsEntity::getCount, false, 1);
        Encoder<EventAttendsEntity> eventAttendsEntity = Encoders.bean(EventAttendsEntity.class);
        Dataset<EventAttendsEntity> attendsDataSet = spark.createDataset(attendsLimit.rdd(), eventAttendsEntity);

        attendsDataSet.show(100);

        /*
        * **********************************************************
        * END
        * **********************************************************
        */
        spark.stop();
    }
}

package com.epam.bigdata.spark;

import com.restfb.*;
import com.restfb.types.Event;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Scanner;

/**
 * Created by Aliaksei_Neuski on 10/5/16.
 */
public class FacebookGrabber {
    
    /*
    * **********************************************************
    * FACEBOOK API CLIENT
    * 
    * https://developers.facebook.com/docs/facebook-login/access-tokens/expiration-and-extension
    * **********************************************************
    */

    public static String CLIEN_ID = "1667952766777543";
    public static String CLIEN_SECRET = "1371366741fe22f1e9f8316707f98864";
    public static String TOKEN =
            "EAAXsZCoQqeMcBANSAvOzh38oZCqnQFUDsptIaNSo4gtU5X8flIgMmuH9qdM0m5qKBjBVbbNDihZBJO6IzOLpt9q37bTQbuJKH2ZCKn456ou6zFpmolqZBLGOdqSZAeDwJhOaZA35HSOeYwP0D9bF3Gr";

    public static String FB_CLIENT_TOKEN_API_CALL = "https://graph.facebook.com/oauth/client_code?"
            + "access_token=" + TOKEN +
            "&client_secret=" + CLIEN_SECRET +
            "&client_id=" + CLIEN_ID +
            "&redirect_uri="
            ;

    public static final String FB_EVENT_FIELDS = "id,attending_count,place,name,description,start_time";
    public static final String DEFAULT_DATE = "2000-01-01";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static String MACHINE_TOKEN;
    private static FacebookClient FB_CLIENT;

    public static void init() {
        String verificationCode = null;
        try {
            verificationCode = new Scanner(new URL(FB_CLIENT_TOKEN_API_CALL).openStream(), "UTF-8")
                    .useDelimiter("\\A").next().replace("{\"code\":\"", "").replace("\"}", "");
        } catch (IOException e) {
            System.err.println(e.getLocalizedMessage());
        }

        try {
            MACHINE_TOKEN = new Scanner(new URL("https://graph.facebook.com/oauth/access_token?code="
                    + verificationCode + "&client_id=" + CLIEN_ID + "&redirect_uri=")
                    .openStream(), "UTF-8")
                    .useDelimiter("\\A")
                    .next()
                    .replace("{\"access_token\":\"", "")
                    .split("\"")[0];
        } catch (IOException e) {
            System.err.println(e.getLocalizedMessage());
        }

        FB_CLIENT = new DefaultFacebookClient(MACHINE_TOKEN, Version.VERSION_2_7);

        // FacebookClient.AccessToken longLivedServerToken = FB_CLIENT.obtainUserAccessToken("1667952766777543", "1371366741fe22f1e9f8316707f98864", "", verificationCode);
    }

    public static FacebookClient getFbClient() {
        if (null == FB_CLIENT) {
            FB_CLIENT = new DefaultFacebookClient(MACHINE_TOKEN, Version.VERSION_2_7);
        }
        return FB_CLIENT;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        init();

        Connection<Event> eventConnection = FacebookGrabber.getFbClient().fetchConnection(
                "search", Event.class,
                Parameter.with("q", "car"),
                Parameter.with("type", "event"),
                Parameter.with("fields", FB_EVENT_FIELDS));

        System.out.println(eventConnection);
    }
}

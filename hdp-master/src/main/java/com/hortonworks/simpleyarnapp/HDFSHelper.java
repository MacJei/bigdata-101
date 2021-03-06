package com.hortonworks.simpleyarnapp;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 9/1/16.
 */
public class HDFSHelper {

    private static Configuration conf;

    static {
        HDFSHelper.initHDFSConf();
    }

    private static void initHDFSConf() {
        conf = new Configuration();
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
    }

    public static int countLines(Path location) throws Exception {
        int num = 0;

        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);

        if (items == null) {
            return num;
        }

        for (FileStatus item: items) {
            // ignoring files like _SUCCESS
            if (item.getPath().getName().startsWith("_")) {
                continue;
            }

            CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream = null;
            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            }
            else {
                stream = fileSystem.open(item.getPath());
            }

            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            String raw = writer.toString();
            num = raw.split("\n").length;
        }

        return num;
    }

    public static List<String> readLines(Path location) throws Exception {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);
        if (items == null) return new ArrayList<>();
        List<String> results = new ArrayList<>();
        for (FileStatus item: items) {
            // ignoring files like _SUCCESS
            if (item.getPath().getName().startsWith("_")) {
                continue;
            }

            CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream;
            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            }
            else {
                stream = fileSystem.open(item.getPath());
            }

            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            String raw = writer.toString();
            Collections.addAll(results, raw.split("\n"));
        }

        return results;
    }

    public static void writeLines(List<String> lines, String filePath) {
        try {
            Path ptOut = new Path(Constants.HDFS_ROOT_PATH + filePath);

            FileSystem fsOut = FileSystem.get(new URI(Constants.HDFS_ROOT_PATH), conf);
            BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fsOut.create(ptOut, true)));

            for (String line : lines) {
                System.out.println(line);
                brOut.write(line);
                brOut.write("\n");
            }

            brOut.close();
            fsOut.close();
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
        }
    }
}

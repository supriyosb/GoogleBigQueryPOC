package com.poc;

import com.google.cloud.bigquery.*;

public class App {

    public static void main(String args[]) throws Exception {
        BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId("bigquery-poc-359808").build().getService();

        final String GET_WORD_COUNT =
                "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE corpus='sonnets' ORDER BY word_count DESC limit 10";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(GET_WORD_COUNT).build();

        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build());

        queryJob = queryJob.waitFor();

        if (queryJob == null){
            throw new Exception("job no longer exists");
        }

        if (queryJob.getStatus().getError() != null){
            throw new Exception(queryJob.getStatus().getError().toString());
        }

        System.out.println("word\tword_count");

        TableResult result = queryJob.getQueryResults();

        for (FieldValueList row : result.iterateAll()){
            String word = row.get("word").getStringValue();
            int wordCount = row.get("word_count").getNumericValue().intValue();
            System.out.printf("%s\t%d\n", word, wordCount);
        }
    }
}

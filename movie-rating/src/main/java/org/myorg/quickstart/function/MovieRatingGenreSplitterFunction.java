package org.myorg.quickstart.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.model.MovieRating;
import org.myorg.quickstart.model.enums.Genre;

import java.util.Collections;

public class MovieRatingGenreSplitterFunction implements FlatMapFunction<MovieRating, MovieRating> {
    @Override
    public void flatMap(MovieRating movieRating, Collector<MovieRating> collector) throws Exception {
        for (Genre genre : movieRating.getGenres()) {
            collector.collect(new MovieRating(movieRating.getName(), Collections.singletonList(genre), movieRating.getRating()));
        }
    }
}
package org.myorg.quickstart.function;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.model.GenreRating;
import org.myorg.quickstart.model.MovieRating;
import org.myorg.quickstart.model.enums.Genre;

public class GenreRatingCollectorFunction implements GroupReduceFunction<MovieRating, GenreRating> {
    @Override
    public void reduce(Iterable<MovieRating> iterable, Collector<GenreRating> collector) throws Exception {
        Genre genre = null;

        int count = 0;
        double totalRating = 0;

        for (MovieRating movieRating : iterable) {
            genre = movieRating.getGenres().get(0);
            count++;
            totalRating += movieRating.getRating();
        }

        collector.collect(new GenreRating(genre, count, totalRating, totalRating / count));
    }
}
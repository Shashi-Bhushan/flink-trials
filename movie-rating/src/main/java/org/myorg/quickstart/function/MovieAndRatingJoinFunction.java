package org.myorg.quickstart.function;

import org.apache.flink.api.common.functions.JoinFunction;
import org.myorg.quickstart.model.Movie;
import org.myorg.quickstart.model.MovieRating;
import org.myorg.quickstart.model.Rating;

public class MovieAndRatingJoinFunction implements JoinFunction<Movie, Rating, MovieRating> {
    @Override
    public MovieRating join(Movie movie, Rating rating) throws Exception {
        return new MovieRating(movie.getName(), movie.getGenres(), rating.getRating());
    }
}
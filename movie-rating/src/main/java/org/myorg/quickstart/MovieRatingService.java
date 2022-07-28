package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.model.GenreRating;
import org.myorg.quickstart.model.Movie;
import org.myorg.quickstart.model.MovieRating;
import org.myorg.quickstart.model.Rating;
import org.myorg.quickstart.model.enums.Genre;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MovieRatingService {
    private static final String RESOURCES_DIR = "src/main/resources/";

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /*
        Get Distinct Genre using

        env.readCsvFile(RESOURCES_DIR + "ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .fieldDelimiter(",")
                .parseQuotedStrings('"')
                .types(Long.class, String.class, String.class)
                .flatMap(new FlatMapFunction<Tuple3<Long, String, String>, Tuple1<String>>() {
                    @Override
                    public void flatMap(Tuple3<Long, String, String> tuple, Collector<Tuple1<String>> collector) throws Exception {
                        Arrays.stream(tuple.f2.split("\\|"))
                                .forEach(genre -> collector.collect(Tuple1.of(genre)));
                    }
                })
                .distinct(0)
                .map(a -> a.f0)
                .print();

        */

        DataSet<Movie> movies = env.readCsvFile(RESOURCES_DIR + "ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .fieldDelimiter(",")
                .parseQuotedStrings('"')
                .types(Long.class, String.class, String.class)
                .map(new MapFunction<Tuple3<Long, String, String>, Movie>() {
                    @Override
                    public Movie map(Tuple3<Long, String, String> tuple) throws Exception {
                        return new Movie(tuple.f0, tuple.f1, Genre.forNames(tuple.f2));
                    }
                });

        DataSet<Rating> rating = env.readCsvFile(RESOURCES_DIR + "ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .fieldDelimiter(",")
                .parseQuotedStrings('"')
                .includeFields(true, true, true, false)
                .types(Long.class, Long.class, Double.class)
                .map(new MapFunction<Tuple3<Long, Long, Double>, Rating>() {
                    @Override
                    public Rating map(Tuple3<Long, Long, Double> tuple) throws Exception {
                        return new Rating(tuple.f0, tuple.f1, tuple.f2);
                    }
                });

        List<GenreRating> sortedGenreRating = movies.join(rating)
                .where(new KeySelector<Movie, Long>() {
                    @Override
                    public Long getKey(Movie movie) throws Exception {
                        return movie.getId();
                    }
                })
                .equalTo(new KeySelector<Rating, Long>() {
                    @Override
                    public Long getKey(Rating rating) throws Exception {
                        return rating.getMovieId();
                    }
                })
                .with(new JoinFunction<Movie, Rating, MovieRating>() {
                    @Override
                    public MovieRating join(Movie movie, Rating rating) throws Exception {
                        return new MovieRating(movie.getName(), movie.getGenres(), rating.getRating());
                    }
                })
                .flatMap(new FlatMapFunction<MovieRating, MovieRating>() {
                    @Override
                    public void flatMap(MovieRating movieRating, Collector<MovieRating> collector) throws Exception {
                        for (Genre genre : movieRating.getGenres()) {
                            collector.collect(new MovieRating(movieRating.getName(), Collections.singletonList(genre), movieRating.getRating()));
                        }
                    }
                })
                .groupBy(new KeySelector<MovieRating, Genre>() {
                    @Override
                    public Genre getKey(MovieRating movieRating) throws Exception {
                        return movieRating.getGenres().get(0);
                    }
                })
                .reduceGroup(new GroupReduceFunction<MovieRating, GenreRating>() {
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
                })
                /*.sortPartition(new KeySelector<GenreRating, Double>() {
                    @Override
                    public Double getKey(GenreRating genreRating) throws Exception {
                        return genreRating.getAverageRating();
                    }
                }, Order.DESCENDING)*/
                .collect()
                .stream().sorted((a, b) -> Double.compare(b.getAverageRating(), a.getAverageRating()))
                .collect(Collectors.toList());

        System.out.println("Sorted Genre Rating " + sortedGenreRating);

    }
}

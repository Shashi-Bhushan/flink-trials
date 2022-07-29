package org.myorg.quickstart;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.function.GenreRatingCollectorFunction;
import org.myorg.quickstart.function.MovieAndRatingJoinFunction;
import org.myorg.quickstart.function.MovieRatingGenreSplitterFunction;
import org.myorg.quickstart.model.GenreRating;
import org.myorg.quickstart.model.Movie;
import org.myorg.quickstart.model.MovieRating;
import org.myorg.quickstart.model.Rating;
import org.myorg.quickstart.model.enums.Genre;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MovieRatingService {
    private static final Logger LOG = Logger.getLogger(MovieRatingService.class.getName());

    private static final String RESOURCES_DIR = "src/main/resources/";

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
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
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

        DataSet<MovieRating> movieRating = getMovieRatingDataset(movies, rating);

        UnsortedGrouping<MovieRating> groupedByGenre = getGroupByGenre(movieRating);

        GenreRating highestRatedGenre = getHighestRatedGenre(groupedByGenre);

        LOG.info("=====> ");
        LOG.log(Level.INFO, "Highest Rated Genre {0}", highestRatedGenre);

        List<MovieRating> highestRatedMoviePerGenre = getHighestRatedMoviePerGenre(groupedByGenre);

        LOG.info("=====> ");
        LOG.log(Level.INFO, "Highest Rated Movie Per Genre {0}", highestRatedMoviePerGenre);
    }

    private static List<MovieRating> getHighestRatedMoviePerGenre(UnsortedGrouping<MovieRating> groupedByGenre) throws Exception {
        return groupedByGenre
                .reduceGroup(new GroupReduceFunction<MovieRating, MovieRating>() {
                    @Override
                    public void reduce(Iterable<MovieRating> iterable, Collector<MovieRating> collector) throws Exception {
                        double highestRating = 0.0d;
                        MovieRating rating = null;

                        for (MovieRating movieRating : iterable) {
                            if (highestRating < movieRating.getRating()) {
                                highestRating = movieRating.getRating();
                                rating = movieRating;
                            }
                        }

                        collector.collect(rating);
                    }
                })
                .name("Highest Rated Movie Per Genre Function")
                .collect();
    }

    private static GenreRating getHighestRatedGenre(UnsortedGrouping<MovieRating> groupedByGenre) throws Exception {
        return groupedByGenre
                // reduce to count, averageRating of each Genre
                .reduceGroup(new GenreRatingCollectorFunction())
                .name("Genre Rating Function")
                /*.sortPartition(new KeySelector<GenreRating, Double>() {
                    @Override
                    public Double getKey(GenreRating genreRating) throws Exception {
                        return genreRating.getAverageRating();
                    }
                }, Order.DESCENDING)*/
                // Collect as Java List to sort in-memory
                .collect()
                .stream().sorted((a, b) -> Double.compare(b.getAverageRating(), a.getAverageRating()))
                .collect(Collectors.toList()).get(0);
    }

    private static UnsortedGrouping<MovieRating> getGroupByGenre(DataSet<MovieRating> movieRating) {
        UnsortedGrouping<MovieRating> groupedByGenre = movieRating
                // Split one MovieRating into multiple, based on individual genres
                .flatMap(new MovieRatingGenreSplitterFunction())
                .name("MovieRating Genre Split Function")
                // Group by Genre
                .groupBy((KeySelector<MovieRating, Genre>) element -> element.getGenres().get(0));
        return groupedByGenre;
    }

    private static JoinOperator<Movie, Rating, MovieRating> getMovieRatingDataset(DataSet<Movie> movies, DataSet<Rating> rating) {
        return movies.join(rating)
                // Join movie and rating on movieId
                .where(Movie::getId)
                .equalTo(Rating::getMovieId)
                .with(new MovieAndRatingJoinFunction())
                .name("Movie and Rating Join Function");
    }
}

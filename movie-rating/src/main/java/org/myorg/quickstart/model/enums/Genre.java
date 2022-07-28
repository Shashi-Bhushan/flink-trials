package org.myorg.quickstart.model.enums;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("java:S115")
public enum Genre {
    Musical("Musical"),
    Thriller("Thriller"),
    Mystery("Mystery"),
    Crime("Crime"),
    Horror("Horror"),
    Romance("Romance"),
    Comedy("Comedy"),
    Fantasy("Fantasy"),
    Adventure("Adventure"),
    Documentary("Documentary"),
    IMAX("IMAX"),
    SciFi("Sci-Fi"),
    Action("Action"),
    War("War"),
    Animation("Animation"),
    Drama("Drama"),
    FilmNoir("Film-Noir"),
    Western("Western"),
    NoGenreListed("(no genres listed)"),
    Children("Children");

    private String name;

    Genre(String name) {
        this.name = name;
    }

    private static final Map<String, Genre> map = Arrays.stream(values())
            .collect(Collectors.toMap(Genre::getName, Function.identity()));

    public String getName() {
        return name;
    }

    public static Genre forName(String name) {
        return map.getOrDefault(name, NoGenreListed);
    }

    public static List<Genre> forNames(String names) {
        return Arrays.stream(names.split("\\|"))
                .map(Genre::forName)
                .collect(Collectors.toList());
    }
}

package org.myorg.quickstart.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.myorg.quickstart.model.enums.Genre;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
public class MovieRating {
    @NonNull
    private String name;

    @NonNull
    private List<Genre> genres;

    @NonNull
    private Double rating;
}

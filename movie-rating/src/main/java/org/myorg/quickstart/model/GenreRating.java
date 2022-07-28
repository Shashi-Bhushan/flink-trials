package org.myorg.quickstart.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.myorg.quickstart.model.enums.Genre;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
public class GenreRating {
    @NonNull
    private Genre genre;

    @NonNull
    private Integer count;

    @NonNull
    private Double totalRating;

    @NonNull
    private Double averageRating;
}

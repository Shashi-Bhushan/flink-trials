package org.myorg.quickstart.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
public class Rating {
    @NonNull
    private Long userId;

    @NonNull
    private Long movieId;

    @NonNull
    private Double rating;
}

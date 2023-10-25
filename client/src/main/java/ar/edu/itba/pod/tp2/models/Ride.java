package ar.edu.itba.pod.tp2.models;

import java.time.LocalDateTime;

public record Ride(
        LocalDateTime startDate,
        int startPk,
        LocalDateTime endDate,
        int endPk,
        boolean isMember
) {
}

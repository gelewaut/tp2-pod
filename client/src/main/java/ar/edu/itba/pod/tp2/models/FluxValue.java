package ar.edu.itba.pod.tp2.models;

public record FluxValue(
        long positive,
        long neutral,
        long negative) {

    @Override
    public String toString() {
        return positive + ";"
                + neutral + ";"
                + negative;
    }
}

package icikic.kstreams.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Player {
    public final long id;
    public final String name;


    public Player(@JsonProperty("id") final long id,
                  @JsonProperty("name") final String name) {
        this.id   = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Player{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

}

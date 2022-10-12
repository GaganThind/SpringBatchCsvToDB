package in.gagan.springbatch.entity;

import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name = "COUNTRIES")
public class Country {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID", nullable = false, unique = true, length = 10)
    private long id;

    @Column(name = "CITY_NAME", nullable = false)
    private String city;

    @Column(name = "STATE_NAME", nullable = false)
    private String state;

    @Column(name = "COUNTRY_NAME", nullable = false)
    private String country;

    @Column(name = "ZIPCODE", nullable = false)
    private long zipcode;

    public long getZipcode() {
        return zipcode;
    }

    public void setZipcode(long zipcode) {
        this.zipcode = zipcode;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Country that = (Country) o;
        return zipcode == that.zipcode &&
                city.equals(that.city) &&
                state.equals(that.state) &&
                country.equals(that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(city, state, country, zipcode);
    }
}

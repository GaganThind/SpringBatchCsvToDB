package in.gagan.springbatch.batch;

import in.gagan.springbatch.entity.Country;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class CountryBatchProcessor implements ItemProcessor<Country, Country> {

    @Override
    public Country process(Country country) throws Exception {
        country.setCountry(StringUtils.capitalize(country.getCountry()));
        country.setState(StringUtils.capitalize(country.getState()));
        country.setCity(StringUtils.capitalize(country.getCity().toLowerCase()));
        return country;
    }
}

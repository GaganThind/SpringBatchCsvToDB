package in.gagan.springbatch.batch;

import in.gagan.springbatch.entity.Country;
import in.gagan.springbatch.repository.CountryRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CountryBatchWriter implements ItemWriter<Country> {

    @Autowired
    private CountryRepository countryRepository;

    private long minValue, maxValue;

    public CountryBatchWriter() { }

    public CountryBatchWriter(long minValue, long maxValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public void write(List<? extends Country> list) throws Exception {

       /* System.out.println("Thread name is: " + Thread.currentThread().getName()
                + " and commit size is " + list.size() + " and minValue is " + minValue
                + " and maxValue is " + maxValue
        );*/
        this.countryRepository.saveAll(list);
    }
}

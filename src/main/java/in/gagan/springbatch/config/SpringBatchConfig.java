package in.gagan.springbatch.config;

import in.gagan.springbatch.batch.CountryBatchProcessor;
import in.gagan.springbatch.batch.CountryBatchWriter;
import in.gagan.springbatch.entity.Country;
import in.gagan.springbatch.partitioner.RangePartitioner;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public SpringBatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job partitionJob() {
        return this.jobBuilderFactory.get("Partitioning-Job")
                .start(partitionStep())
                .build();
    }

    @Bean
    public Step partitionStep() {
        return this.stepBuilderFactory.get("Partitioning-Step")
                .partitioner(importCountryDataWorkerStep().getName(), rangePartitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    public Partitioner rangePartitioner() {
        long max = 155598;
        /*try {
            max = Files.lines(
                    Paths.get("src/main/resources/Data.csv"),
                    Charset.defaultCharset()
            ).count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/

        return new RangePartitioner((int) max);
    }

    @Bean
    public PartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setGridSize(4);
        partitionHandler.setTaskExecutor(taskExecutor());
        partitionHandler.setStep(importCountryDataWorkerStep());
        return partitionHandler;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    public Step importCountryDataWorkerStep() {
        return this.stepBuilderFactory.get("Import-Country-Data-Slave-Step")
                .<Country, Country>chunk(500)
                .reader(countryFileItemReader())
                .processor(countryItemProcessor())
                .writer(countryItemWriter(0, 0))
                .build();
    }

    @Bean
    public FlatFileItemReader<Country> countryFileItemReader() {
        FlatFileItemReader<Country> countryFlatFileItemReader = new FlatFileItemReader<>();
        countryFlatFileItemReader.setResource(new FileSystemResource("src/main/resources/Data.csv"));
        countryFlatFileItemReader.setName("csvFileReader");
        countryFlatFileItemReader.setLinesToSkip(1);
        countryFlatFileItemReader.setLineMapper(lineMapper());
        return countryFlatFileItemReader;
    }

    @Bean
    public LineMapper<Country> lineMapper() {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("city", "state", "country", "zipcode");

        BeanWrapperFieldSetMapper<Country> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Country.class);

        DefaultLineMapper<Country> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public ItemProcessor<Country, Country> countryItemProcessor() {
        return new CountryBatchProcessor();
    }

    @Bean
    @StepScope
    public ItemWriter<Country> countryItemWriter(
            @Value("#{stepExecutionContext[minValue]}") long minValue,
            @Value("#{stepExecutionContext[maxValue]}") long maxValue
    ) {
        return new CountryBatchWriter(minValue, maxValue);
    }
}

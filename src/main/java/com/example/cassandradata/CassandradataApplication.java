package com.example.cassandradata;

import com.example.cassandradata.author.Author;
import com.example.cassandradata.author.AuthorRepository;
import com.example.cassandradata.book.Book;
import com.example.cassandradata.book.BookRepository;
import com.example.cassandradata.config.DataStaxAstraProperties;
import com.example.cassandradata.security.Equity;
import com.example.cassandradata.security.EquityRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@EnableConfigurationProperties(DataStaxAstraProperties.class)
@SpringBootApplication
public class CassandradataApplication {

    public static void main(String[] args) {
        SpringApplication.run(CassandradataApplication.class, args);
    }

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

    @Autowired
    private EquityRepository equityRepository;

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorFileLocation;

    @Value("${datadump.location.works}")
    private String worksFileLocation;

    @PostConstruct
    public void processEquities() {
        loadSecurity();
        initAuthors();
        initWorks();
    }

    @SneakyThrows
    private void initAuthors() {
        Path path = getPath(authorFileLocation);
        try (Stream<String> lines = Files.lines(path)) {
            log.info("Inserting Authors");
            lines.parallel().
                map(this::getAuthor).
                map(authorRepository::save).
                forEach(a -> log.info("Author {} is inserted", a.getName()));
            log.info("All authors were inserted");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private Path getPath(String p) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource(p);
        Path path = Paths.get(resource.toURI());
        return path;
    }

    private Author getAuthor(String line) {
        try {
            String jsonRow = line.substring(line.indexOf("{"));
            JSONObject jsonObject = new JSONObject(jsonRow);
            String key = jsonObject.optString("key", "/K");
            String name = jsonObject.optString("name");
            return new Author(key.substring(key.lastIndexOf("/") + 1), name, name);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Book getBook(String line) {
        try {
            String jsonRow = line.substring(line.indexOf("{"));
            log.info("jsonRow:::" + jsonRow);
            JSONObject jsonObject = new JSONObject(jsonRow);
            String key = jsonObject.optString("key").replace("/works/", "");
            Book book = new Book();
            book.setId(key);
            book.setName(jsonObject.optString("title"));

            JSONObject descObj = jsonObject.optJSONObject("description");
            if (!Objects.isNull(descObj)) {
                book.setDescription(descObj.optString("value"));
            }

            JSONObject createdObj = jsonObject.optJSONObject("created");
            if (!Objects.isNull(createdObj)) {
                String pubslishedDateStr = createdObj.optString("value");
                if (!Objects.isNull(pubslishedDateStr)) {
                    book.setPublishedDate(LocalDate.parse(pubslishedDateStr, formatter));
                }
            }
            JSONArray coversJsonAry = jsonObject.optJSONArray("covers");
            if (!Objects.isNull(coversJsonAry)) {
                var coverIds = new ArrayList<String>();
                for (int i = 0; i < coversJsonAry.length(); i++) {
                    coverIds.add(coversJsonAry.optString(i));
                }
                book.setCoverIds(coverIds);
            }

            JSONArray authorsJsonAry = jsonObject.optJSONArray("authors");
            if (authorsJsonAry != null) {
                var authorIds = new ArrayList<String>();
                for (int i = 0; i < authorsJsonAry.length(); i++) {
                    String authorId = authorsJsonAry.getJSONObject(i).
                        getJSONObject("author").
                        getString("key").
                        replace("/authors/", "");
                    authorIds.add(authorId);
                }
                book.setAuthorIds(authorIds);
                var authorNames = authorIds
                    .stream()
                    .map(authorRepository::findById)
                    .map(optAuthor -> optAuthor.isPresent() ? optAuthor.get().getName() : "Unknown Author")
                    .collect(Collectors.toList());
                book.setAuthorNames(authorNames);
            }
            return book;

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @SneakyThrows
    private void initWorks() {
        Path path = getPath(worksFileLocation);
        try (Stream<String> lines = Files.lines(path)) {
            log.info("Inserting Books");
            lines.parallel().
                map(this::getBook).
                map(bookRepository::save).
                forEach(b -> log.info("book {} is saved", b.getName()));
            log.info("All Books data is inserted");
        } catch (IOException e) {
            log.error("error occurred while inserting books data", e);
            System.exit(1);
        }
    }

    private void loadSecurity() {
        for (int i = 120; i < 120; i++) {
            Equity equity = new Equity("MSFT"+i,"Amamzon"+i, "cusip"+1, "isin"+1, "sedol");
            equityRepository.insert(equity);
        }

        List<Equity> equityList = equityRepository.findAll();
        equityList.forEach(eq-> log.info("equity {}", eq));

    /*    ResultSet rs = session.execute("select * from security.Equity");
        List<Row> all = rs.all();
        for (Row row1 : all) {
            System.out.println(row1.getString("equitysymbol") + " ::" + row1.getString("description"));
        }*/
    }
}

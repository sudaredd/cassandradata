package com.example.cassandradata.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.example.cassandradata.CassandradataApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.nio.file.Path;

@Configuration
public class CassandraConfiguration {

    @Value("${ASTRA_DB_CLIENT_ID}")
    private String clientId;

    @Value("${ASTRA_DB_CLIENT_SECRET}")
    private String clientSecret;

    @Value("${ASTRA_DB_KEYSPACE}")
    private String keySpace;

/*    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.
            withKeyspace(keySpace).
            withCloudSecureConnectBundle(bundle);
    }*/

    @Bean
    CqlSession session(DataStaxAstraProperties astraProperties) {
        String propsFile = "/"+astraProperties.getSecureConnectBundle().getName();
        CqlSession session = CqlSession.builder()
            .withCloudSecureConnectBundle(CassandradataApplication.class.
                getResourceAsStream(propsFile))
            .withAuthCredentials(clientId, clientSecret)
            .withKeyspace(keySpace)
            .build();
        ResultSet rs = session.execute("select release_version from system.local");
        Row row = rs.one();
        //Print the results of the CQL query to the console:
        if (row != null) {
            System.out.println(row.getString("release_version"));
        } else {
            System.out.println("An error occurred.");
        }

        System.out.println("session is ," + session);
        return session;
    }
}

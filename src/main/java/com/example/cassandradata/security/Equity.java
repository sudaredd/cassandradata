package com.example.cassandradata.security;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import static org.springframework.data.cassandra.core.mapping.CassandraType.Name.TEXT;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
@Table(value = "Equity")
public class Equity {

    @Id
    @PrimaryKeyColumn(name = "equity_symbol", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    @CassandraType(type = TEXT)
    @Column("equity_symbol")
    private String equitySymbol;


    @Column("description")
    @CassandraType(type = TEXT)
    private String description;

    @Column("cusip")
    @CassandraType(type = TEXT)
    private String cusip;

    @Column("isin")
    @CassandraType(type = TEXT)
    private String isin;

    @Column("sedol")
    @CassandraType(type = TEXT)
    private String sedol;

}

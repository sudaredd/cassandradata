package com.example.cassandradata.security;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EquityRepository extends CassandraRepository<Equity, String> {

}

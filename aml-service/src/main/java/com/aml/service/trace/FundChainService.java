package com.aml.service.trace;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

/**
 * Fund chain traceability service.
 * Traces money flow through N hops using ClickHouse as the transaction store.
 *
 * Design spec: "Medium/high-risk alerts trigger fund chain traceability (last N hops)"
 * Anti-skew: "timeout and depth limits on traceability queries to prevent chain explosion"
 */
@Service
public class FundChainService {

    @Value("${aml.clickhouse.jdbc-url}")
    private String clickhouseUrl;

    @Value("${aml.clickhouse.username:default}")
    private String clickhouseUser;

    @Value("${aml.clickhouse.password:}")
    private String clickhousePassword;

    private static final int DEFAULT_MAX_HOPS = 5;
    private static final int DEFAULT_MAX_RESULTS = 100;
    private static final int QUERY_TIMEOUT_SECONDS = 30;

    /**
     * Trace forward: where did the money go from this customer?
     * Returns a list of hops, each hop is a list of transactions.
     */
    public List<List<Map<String, Object>>> traceForward(String customerId, int maxHops) {
        return trace(customerId, maxHops, true);
    }

    /**
     * Trace backward: where did the money come from to this customer?
     */
    public List<List<Map<String, Object>>> traceBackward(String customerId, int maxHops) {
        return trace(customerId, maxHops, false);
    }

    /**
     * Trace the fund chain in either direction.
     * Returns list of hops. Each hop is a list of {txn_id, counterparty, amount, timestamp}.
     */
    private List<List<Map<String, Object>>> trace(String startCustomerId, int maxHops, boolean forward) {
        if (maxHops <= 0) maxHops = DEFAULT_MAX_HOPS;
        if (maxHops > 10) maxHops = 10; // Safety cap

        List<List<Map<String, Object>>> chain = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        visited.add(startCustomerId);

        String currentCustomerId = startCustomerId;

        for (int hop = 0; hop < maxHops; hop++) {
            List<Map<String, Object>> hopTransactions;

            if (forward) {
                hopTransactions = getOutgoingTransactions(currentCustomerId);
            } else {
                hopTransactions = getIncomingTransactions(currentCustomerId);
            }

            if (hopTransactions.isEmpty()) break;

            // Filter out already-visited counterparties to prevent cycles
            List<Map<String, Object>> filtered = new ArrayList<>();
            for (Map<String, Object> txn : hopTransactions) {
                String cp = txn.get("counterparty").toString();
                if (!visited.contains(cp)) {
                    filtered.add(txn);
                    visited.add(cp);
                }
            }

            if (filtered.isEmpty()) break;
            chain.add(filtered);

            // Move to the most significant counterparty (highest amount)
            currentCustomerId = filtered.stream()
                .max(Comparator.comparingDouble(t -> ((Number) t.get("amount")).doubleValue()))
                .map(t -> t.get("counterparty").toString())
                .orElse(null);

            if (currentCustomerId == null) break;
        }

        return chain;
    }

    private List<Map<String, Object>> getOutgoingTransactions(String customerId) {
        String sql = "SELECT txn_id, customer_id, counterparty_id, amount_usd, txn_type, channel, timestamp " +
                     "FROM txn_chain_view WHERE customer_id = ? " +
                     "ORDER BY amount_usd DESC LIMIT ?";
        return queryTransactions(sql, customerId);
    }

    private List<Map<String, Object>> getIncomingTransactions(String customerId) {
        String sql = "SELECT txn_id, customer_id, counterparty_id, amount_usd, txn_type, channel, timestamp " +
                     "FROM txn_chain_view WHERE counterparty_id = ? " +
                     "ORDER BY amount_usd DESC LIMIT ?";
        return queryTransactions(sql, customerId);
    }

    private List<Map<String, Object>> queryTransactions(String sql, String customerId) {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, customerId);
            ps.setInt(2, DEFAULT_MAX_RESULTS);
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= cols; i++) {
                        row.put(meta.getColumnLabel(i), rs.getObject(i));
                    }
                    // Normalize key names for the chain view
                    Map<String, Object> txn = new HashMap<>();
                    txn.put("txn_id", row.get("txn_id"));
                    txn.put("customer", row.get("customer_id"));
                    txn.put("counterparty", row.get("counterparty_id"));
                    txn.put("amount", row.get("amount_usd"));
                    txn.put("type", row.get("txn_type"));
                    txn.put("channel", row.get("channel"));
                    txn.put("timestamp", row.get("timestamp"));
                    results.add(txn);
                }
            }
        } catch (SQLException e) {
            // Log and return empty - don't fail the whole request for trace issues
            System.err.println("Fund chain query failed: " + e.getMessage());
        }
        return results;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword);
    }
}

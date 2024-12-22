package tech.ydb.samples.updater1;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import tech.ydb.jdbc.exception.YdbRetryableException;

/**
 *
 * @author zinal
 */
public class Updater1 implements Runnable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Updater1.class);

    private final Connection con;
    private final BufferedReader input;
    private final int batchSize;
    private long lastReportedAt = 0L;
    private long rows = 0L;
    private long transactions = 0L;
    private long retries = 0L;

    public Updater1(Connection con, BufferedReader input, int batchSize) {
        this.con = con;
        this.input = input;
        this.batchSize = batchSize;
    }

    public long getRows() {
        return rows;
    }

    public long getTransactions() {
        return transactions;
    }

    public long getRetries() {
        return retries;
    }

    @Override
    public void run() {
        lastReportedAt = System.currentTimeMillis();
        try {
            List<Long> batch = new ArrayList<>();
            String line;
            while ((line = input.readLine()) != null) {
                line = line.trim();
                if (line.length()==0) {
                    continue;
                }
                batch.add(Long.valueOf(line));
                if (batch.size() >= batchSize) {
                    updateBatchWithRetries(batch);
                    batch.clear();
                    maybeReport(false);
                }
            }
            updateBatchWithRetries(batch);
            maybeReport(true);
        } catch(Exception ex) {
            throw new RuntimeException("run() failed", ex);
        }
    }

    private void updateBatchWithRetries(List<Long> batch) throws Exception {
        while (true) {
            try {
                updateBatch(batch);
                return;
            } catch(YdbRetryableException yre) {
                retries += 1;
            }
            try {
                // Примитивная пауза при повторе.
                // На практике используются специальные библиотеки управления повторами.
                Thread.sleep(ThreadLocalRandom.current().nextLong(50L, 500L));
            } catch(InterruptedException ix) {}
        }
    }

    private void updateBatch(List<Long> batch) throws Exception {
        if (batch.isEmpty()) {
            return;
        }
        String sql = "UPDATE `sm1/EVENTS` SET REMARKS=? WHERE CODE=?";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            for (Long code : batch) {
                ps.setString(1, "updated #" + code);
                ps.setLong(2, code);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        con.commit();
        transactions += 1;
        rows += batch.size();
    }

    private boolean maybeReport(boolean term) {
        boolean needReport = term;
        if (!needReport) {
            long tv = System.currentTimeMillis();
            if (tv - lastReportedAt >= 10000L) {
                needReport = true;
                lastReportedAt = tv;
            }
        }
        if (needReport) {
            LOG.info("Progress: {} transactions with {} retries, {} rows updated.",
                    transactions, retries, rows);
        }
        return needReport;
    }

    private static Properties readProperties(String fileName) {
        byte[] data;
         try {
             data = Files.readAllBytes(Paths.get(fileName));
         } catch (IOException ix) {
             throw new RuntimeException("Failed to read file " + fileName, ix);
         }
         Properties props = new Properties();
         try {
             props.loadFromXML(new ByteArrayInputStream(data));
         } catch (IOException ix) {
             throw new RuntimeException("Failed to parse properties file " + fileName, ix);
         }
         return props;
    }

    private static Connection createConnection(Properties props) throws Exception {
        String url = props.getProperty("ydb.url");
        String username = props.getProperty("ydb.username");
        if (username != null && username.length() > 0) {
            String password = props.getProperty("ydb.password");
            return DriverManager.getConnection(url, username, password);
        } else {
            return DriverManager.getConnection(url);
        }
    }

    public static void main(String[] args) {
        try {
            String configFileName = "updater1.xml";
            String idFileName = "updater1.txt";
            if (args.length > 0) {
                configFileName = args[0];
            }
            if (args.length > 1) {
                idFileName = args[1];
            }
            LOG.info("Reading configuration from {}...", configFileName);
            Properties props = readProperties(configFileName);
            long tvStart, tvFinish, transactions, retries;
            LOG.info("Opening the id file {}...", idFileName);
            try (FileInputStream fis = new FileInputStream(idFileName)) {
                LOG.info("Obtaining the JDBC connection...");
                try ( Connection con = createConnection(props) ) {
                    con.setAutoCommit(false);
                    LOG.info("Creating the worker instance...");
                    Updater1 worker = new Updater1(con,
                            new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8)),
                            1000
                    );
                    LOG.info("Entering the updater algorithm...");
                    tvStart = System.currentTimeMillis();
                    worker.run();
                    tvFinish = System.currentTimeMillis();
                    transactions = worker.getTransactions();
                    retries = worker.getRetries();
                }
            }
            LOG.info("All done in {} milliseconds, total {} transactions with {} retries.",
                    tvFinish - tvStart, transactions, retries);
        } catch(Exception ex) {
            LOG.error("FATAL", ex);
        }
    }

}
